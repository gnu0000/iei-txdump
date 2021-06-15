#!perl
# Craig Fitzgerald

use warnings;
use strict;
use File::Basename;
use lib dirname(__FILE__);
use lib dirname(__FILE__) . "/lib";
use File::Copy;
use Gnu::ArgParse;
use Gnu::FileUtil qw(SlurpFile SpillFile);

MAIN:
   $| = 1;
   ArgBuild("*^debug *^help ?");
   ArgParse(@ARGV) or die ArgGetError();
   Patch();
   exit(0);


# files               <-- param
#    100                  dirs1 (index)
#      100*               dirs2 (account)
#         index.csv
# 
sub Patch {
   my $root = ArgGet();

   my @dirs = GatherDirs($root);
   foreach my $dir (@dirs) {
      ProcessAccounts("$root\\$dir");
   }
}


sub ProcessAccounts {
   my ($root) = @_;

   my @dirs = GatherDirs($root);
   foreach my $dir (@dirs) {
      ProcessAccount("$root\\$dir");
   }
}

sub ProcessAccount {
   my ($root) = @_;

   my @files = GatherFiles($root);

   return unless scalar @files == 1;
   my $file = $files[0];
   return unless $file =~ /^index\.csv/i;

   print "Removing account dir: '$root'\n";
   unlink("$root\\index.csv");
   rmdir("$root");
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


sub GatherFiles {
   my ($root) = @_;

   opendir(my $dh, $root) or die "cant open dir '$root'!";
   my @all = readdir($dh);
   closedir($dh);
   my @files = ();
   foreach my $entry (@all) {
      push (@files, $entry) if -f "$root\\$entry";
   }
   return @files;
}
