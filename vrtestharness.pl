#! /usr/bin/perl -w
use IPC::Open3 qw(open3);

mkdir("phtest") if !-d "phtest";

if ($ARGV[0] eq "start") {
    system("./mpvr -cphtest/c.js -kall") if -r "phtest/c.js";
    truncate("phtest/log", 0);
    unlink("phtest/c.js");

    open(CONFIG, ">phtest/c.js") || die;
    print CONFIG "{\"members\":{";
    for ($i = 0; $i < $ARGV[1]; ++$i) {
        print CONFIG ($i ? ",\n" : ""), "\"n$i\":{\"path\":\"phtest/n$i\"}";
    }
    print CONFIG "}}\n";
    close CONFIG;

    for ($i = 0; $i < $ARGV[1]; ++$i) {
        system("./mpvr -cphtest/c.js -rn$i >>phtest/log 2>&1 &");
    }
} elsif ($ARGV[0] eq "kill" && (@ARGV == 1 || $ARGV[1] eq "all")) {
    if (-r "phtest/c.js") {
        system("./mpvr -cphtest/c.js -kall");
    }
} elsif ($ARGV[0] eq "kill") {
    system("./mpvr -cphtest/c.js -kn$ARGV[1]");
} elsif ($ARGV[0] eq "revive") {
    system("./mpvr -cphtest/c.js -rn$ARGV[1] >>phtest/log 2>&1 &");
} elsif ($ARGV[0] eq "do" && $ARGV[1] eq "createfile") {
    system("./mpvr", "-cphtest/c.js", "-mn$ARGV[2]", "write", $ARGV[3], $ARGV[4]);
} elsif ($ARGV[0] eq "verify") {
    $pid = open3(\*CIN, \*COUT, \*CERR,
                 "./mpvr", "-cphtest/c.js", "-mn$ARGV[1]", "read", $ARGV[2]);
    waitpid($pid, 0);
    undef $/;
    $outbuf = <COUT>;
    close CIN;
    close COUT;
    close CERR;
    $/ = "\n";
    chomp($outbuf);
    chomp($ARGV[3]);
    exit($ARGV[3] eq $outbuf ? 0 : 1);
} else {
    print STDERR "./vrtestharness.pl: Bad command\n";
    exit 1;
}

