package CATS::Git;
use strict;
use warnings;

use Fcntl qw(:DEFAULT :flock);
use IO::Handle;
use Error qw(:try);
use Git;
use CATS::DB;
use CATS::BinaryFile;
use POSIX qw(strftime);

my $cats_git_storage;

sub set_new_root {
    local $/ = '/';
    ($cats_git_storage) = @_;
    chomp($cats_git_storage);
}

BEGIN {
    use Exporter;
    our @ISA = qw(Exporter);

    our @EXPORT = qw(
                        cpa_from_source_info
                        put_problem_zip
                        get_problem_zip
                        get_problem_history
                        escape_cmdline
                        diff_files

                        contest_repository_path
                        problem_repository_path
                        contest_repository
                        problem_repository
                        create_contest_repository
                        create_problem_repository

                        put_source_in_repository
                        get_source_from_hash
                        get_log_dump_from_hash
                        set_new_root
                   );

    our %EXPORT_TAGS = (all => [ @EXPORT ]);

    set_new_root($ENV{CATS_GIT_STORAGE} || './cats-git');
}

# cpa = contest problem account
sub cpa_from_source_info {
    return ($_[0]{contest_id}, $_[0]{problem_id}, $_[0]{account_id});
}

sub escape_cmdline {
    my $esc = shift;
    $esc =~ s/\'/\\\'/g;
    return $esc;
}

sub lock_repository($) {
    my ($repo) = @_;
    sysopen my $fh, "$$repo{opts}{WorkingCopy}/repository.lock", O_WRONLY|O_CREAT;
    flock $fh, LOCK_EX;
    return $fh;
}

sub unlock_repository($) {
    my ($fh) = @_;
    flock $fh, LOCK_UN;
    close $fh;
}

sub create_repository($) {
    Git::command(init => @_);
}

sub get_repository($) {
    return Git->repository(Directory => @_);
}

sub put_problem_zip {
    my ($pid, $zip) = @_;
    my $repo = problem_repository($pid);
    my ($login, $email) = $dbh->selectrow_array("SELECT login, email FROM accounts WHERE id=?", undef, $CATS::Misc::uid);

    my $lock = lock_repository $repo;
    $repo->command("rm", "-f", "--ignore-unmatch", "*");
    $zip->extractTree('', $repo->wc_path);
    $repo->command("add", "*");
    # We don't need a lock file
    $repo->command("rm", "-f", "--ignore-unmatch" , "--cached", "repository.lock");
    $login = escape_cmdline($login);
    $email = escape_cmdline($email);
    eval {
        $repo->command(commit => "--author='$login <$email>'", '-m', '$CATS::Misc::uid');
    }; # may remind unchanged
    unlock_repository $lock;
}

sub get_problem_zip {
    my ($pid, $file, $revision) = @_;
    $revision ||= "HEAD";
    my $repo = problem_repository($pid);
    $repo->command("archive", "-o", "$file", $revision);
}

sub contest_repository_path($) {
    return "$cats_git_storage/contests/$_[0]";
}

sub problem_repository_path($) {
    return "$cats_git_storage/problems/$_[0]";
}

sub contest_repository($) {
    return get_repository contest_repository_path $_[0];
}

sub problem_repository($) {
    return get_repository problem_repository_path $_[0];
}

sub create_contest_repository($) {
    return create_repository contest_repository_path $_[0];
}

sub create_problem_repository($) {
    return create_repository problem_repository_path $_[0];
}

sub get_source_from_hash {
    my ($cid, $hash) = @_;
    my $r = contest_repository($cid);
    $r->command("checkout", "--force", "master");
    return $r->command("show", $hash); 
}

sub put_source_in_repository {
    my ($cid, $pid, $aid, $src) = @_;
    my $repo = contest_repository($cid);
    my $fname = "$pid/$aid";

    my $lock = lock_repository($repo);

    CATS::BinaryFile::save($repo->wc_path . '/' . $fname, $src);

    my ($login, $email) = $dbh->selectrow_array(qq~
        SELECT
            login, email
        FROM accounts
        WHERE id = ?~, {}, $aid);
    my $hash = $repo->command("hash-object", "-w", $repo->wc_path . $fname);
    #hash_and_insert_object($repo->wc_path . $fname);#$repo->wc_path() . '/' . $fname);
    # dunno why, but Git.pm refuses to hash it, so screw it
    $login = escape_cmdline($login);
    $email = escape_cmdline($email);
    $repo->command("add", $fname);
    eval {
        $repo->command("commit", "--author='$login <$email>'",  '-m', '$aid');
    };
    open(HEAD, "<", "$$repo{opts}{Repository}/refs/heads/master");
    my $revision = <HEAD>;
    close HEAD;

    unlock_repository($lock);

    return ($revision, $hash);
}

sub get_log_dump_from_hash {
    my ($cid, $hash) = @_;
    return contest_repository($cid)->command("show", $hash);
}

sub diff_files {
    my ($cid1, $pid1, $uid1, $revision1, $cid2, $pid2, $uid2, $revision2) =  @_;
    my $a = contest_repository_path($cid1) . "/$pid1/$uid1";
    my $b = contest_repository_path($cid2) . "/$pid2/$uid2";
    my ($r1, $r2, $lock1, $lock2);
    my $res;
    my @prefix = (diff => "-U65536");
    my @ret;
    $r1 = contest_repository($cid1);
    if ($a ne $b)
    {
        $r2 = contest_repository($cid2);
        $lock1 = lock_repository $r1;
        $lock2 = lock_repository $r2 if $cid1 != $cid2;
        $r1->command(checkout => $revision1, $a);
        $r2->command(checkout => $revision2, $b);
    }

    try {
        $res = $a ne $b ? Git::command(@prefix , $a, $b)
          : $r1->command(@prefix, $revision1, $revision2, $a);
    }
    catch Git::Error::Command with {
        my $E = shift;
        CATS::BinaryFile::save("/tmp/dump", $E->cmdline);
        $res = $E->cmd_output;
    };
    unless ($res)
    {
        push @ret, "@@";
        open my $fh, "<", $a;
        while(<$fh>)
        {
            chomp;
            push @ret, " $_";
        }
    }
    else
    {
        @ret = split(/\r?\n/, $res); # respect any line endings
    }

    if ($a ne $b)
    {
        $r1->command(checkout => "--force", "master");
        $r2->command(checkout => "--force", "master");
        unlock_repository $lock1;
        unlock_repository $lock2 if $cid1 != $cid2;
    }

    return @ret;
}

sub get_problem_history($) {
    my $r = problem_repository($_[0]);
    my @hashes = $r->command(log => "--pretty=%H");
    my @logins = $r->command(log => "--pretty=%aN");
    my @emails = $r->command(log => "--pretty=%aE");
    my @dates = $r->command(log => "--pretty=%at");
    my @result = ();
    for(my $i = 0; $i < @hashes; ++$i)
    {
        my ($hash, $login, $email, $date) = ($hashes[$i], $logins[$i], $emails[$i], $dates[$i]);
        push @result, {
                       hash => $hash,
                       login => $login,
                       email => $email,
                       date => strftime("%d.%m.%Y %H:%M:%S", localtime($date)),
        };
    }
    return @result;
}

1;
