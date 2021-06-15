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


my $HEADER = "accountNumber,packetType,filename,archiveDate,packetId,fileFormat,invoiceNumber,cmDocumentId\n";

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

   my $target = "$root\\index.csv";
   return unless -f $target;
   print "Patching '$target'\n";

   my $data = $HEADER . SlurpFile($target);
   move($target, $target . ".old") or die "Cant move $root";
   SpillFile($target, $data);
   unlink($target . ".old");
}


sub GatherDirs {
   my ($root) = @_;

   opendir(my $dh, $root) or die "cant open dir '$root'!";
   my @all = readdir($dh);
   closedir($dh);
   my @dirs = ();
   foreach my $entry (@all) {
      push (@dirs, $entry) if -d "$root\\$entry" && $entry ne "..";
   }
   return @dirs;
}
