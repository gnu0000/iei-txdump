#!perl
#
# CountAccounts.pl  -  Traverses dirs, Counts all the acccounts (~160,000 so far)
# Craig Fitzgerald

use warnings;
use strict;
use File::Basename;
use lib dirname(__FILE__);
use lib dirname(__FILE__) . "/lib";
use File::Copy;
use Gnu::ArgParse;
use Gnu::FileUtil qw(SlurpFile SpillFile);

my $COUNT = 0;

MAIN:
   $| = 1;
   ArgBuild("*^debug *^help ?");
   ArgParse(@ARGV) or die ArgGetError();
   Fish();
   print "Found $COUNT account dirs.\n";
   exit(0);


# files               <-- param
#    100                  dirs1 (index)
#      100*               dirs2 (account)
sub Fish {
   my $root = ArgGet();

   my @dirs = GatherDirs($root);
   foreach my $dir (@dirs) {
      ProcessAccounts("$root\\$dir");
   }
}

sub ProcessAccounts {
   my ($root) = @_;

   my @dirs = GatherDirs($root);
   my $dirCount = scalar @dirs;
   $COUNT += $dirCount;
   print "$dirCount dirs in $root\n";
}

sub GatherDirs {
   my ($root) = @_;

   opendir(my $dh, $root) or die "cant open dir '$root'!";
   my @all = readdir($dh);
   closedir($dh);
   my @dirs = ();
   foreach my $entry (@all) {
      push (@dirs, $entry) if -d "$root\\$entry" && $entry ne ".." && $entry ne ".";
   }
   return @dirs;
}
