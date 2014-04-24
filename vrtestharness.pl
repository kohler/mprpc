#! /usr/bin/perl -w
use IPC::Open3 qw(open3);

mkdir("phtest") if !-d "phtest";

if ($ARGV[0] eq "start" || $ARGV[0] eq "startnodes") {
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
        system("./mpvr -cphtest/c.js -rn$i --log phtest/log &");
    }
} elsif (($ARGV[0] eq "kill" || $ARGV[0] eq "stop")
         && (@ARGV == 1 || $ARGV[1] eq "all")) {
    if (-r "phtest/c.js") {
        system("./mpvr -cphtest/c.js -kall");
    }
} elsif ($ARGV[0] eq "kill" || $ARGV[0] eq "stop") {
    system("./mpvr -cphtest/c.js -kn$ARGV[1]");
} elsif ($ARGV[0] eq "revive") {
    system("./mpvr -cphtest/c.js -rn$ARGV[1] --log phtest/log &");
} elsif ($ARGV[0] eq "do" && $ARGV[1] eq "createfile") {
    system("./mpvr", "-cphtest/c.js", "-mn$ARGV[2]", "write", $ARGV[3], $ARGV[4]);
} elsif ($ARGV[0] eq "verify") {
    $pid = open3(\*CIN, \*COUT, \*CERR,
                 "./mpvr", "-cphtest/c.js", "-mn$ARGV[1]", "--log=/dev/stderr", "read", $ARGV[2]);
    waitpid($pid, 0);
    undef $/;
    $outbuf = <COUT>;
    close CIN;
    close COUT;
    close CERR;
    $/ = "\n";
    chomp($outbuf);
    chomp($ARGV[3]);
    if ($ARGV[3] eq $outbuf) {
        exit(0);
    } else {
        print STDERR "./mpvr read $ARGV[2]: expected $ARGV[3], got $outbuf\n";
        exit(1);
    }
} else {
    print STDERR "./vrtestharness.pl: Bad command \"", join(" ", @ARGV), "\"\n";
    exit 1;
}

