#!perl
#
# This script gets all TX correspondence from CorrespondenceArchive
# and dumps them to a directory tree
# 05/18/2021
#
# Craig Fitzgerald

use warnings;
use strict;
use feature 'state';
use File::Basename;
use lib dirname(__FILE__);
use lib dirname(__FILE__) . '/lib';
use JSON;
use Time::HiRes qw(time);
use Gnu::SimpleDB;
use Gnu::Template qw(Usage);
use Gnu::ArgParse;
use Gnu::DebugUtil qw(DumpRef);
use Gnu::FileUtil qw(SlurpFile SpillFile);
use Common qw(MSConnect);

my $STATS = 
      {
      totalaccounts => 0,
      accounts      => 0,
      files         => 0,
      start         => time()
      };

MAIN:
   $| = 1;

   ArgBuild("*^env= *^test= *^start= *^end= *^account= *^suffix= *^list *^skipheaders *^debug *^verbose *^help");
   ArgParse(@ARGV) or die ArgGetError();
   Usage() if ArgIs("help") || !ArgIs();

   my $server = GetServer();
   print "Connecting to $server\n";
   my $db = MSConnect($server, "NY");

   my $accounts = ArgIs("account") ? [ArgGet("account")] : GetAccounts($db);
   ListAccounts($accounts) if ArgIs("list");

   foreach my $account (@{$accounts}) {
      ProcessAccount($account);
      last if ArgIs("test") && $STATS->{accounts} >= ArgGet("test");
   }
   print "Processed $STATS->{accounts} accounts.\n";
   exit(0);


sub GetAccounts {
   my ($db) = @_;

   my $sql = "SELECT IEINum FROM serviceaddr WHERE State='TX'";
   my $rows = FetchArray($db, $sql);
   my @accounts = map{$_->{IEINum}} (@{$rows});
   
   if (ArgIsAny("start", "end")) {
      my @tmp = ();
      my $start = ArgGet("start") || 0;
      my $end   = ArgGet("end"  ) || 10000000000;
      foreach my $account (@accounts) {
         next if $account < $start || $account >= $end;
         push (@tmp, $account);
      }
      @accounts = @tmp;
   }
   @accounts = sort @accounts;

   $STATS->{totalaccounts} = scalar @accounts;
   return \@accounts;
}


sub ProcessAccount {
   my ($account) = @_;

   DumpAccountFiles($account);
   $STATS->{accounts}++;
   ShowStats();
}


sub DumpAccountFiles {
   my ($account) = @_;

   my $dir = MakeAccountDir($account);
   my $index = GetFileIndex($account);
   my @entries = sort {$a->{archiveDate} cmp $b->{archiveDate}} @{$index};
   print "(" . scalar @entries . " files)\n";

   open (my $localIndex, ">", "$dir\\index.csv");
   WriteIndexHeader($localIndex);
   map {DumpFile($_, $dir, $localIndex)} @entries;
   close ($localIndex);
}


sub GetFileIndex {
   my ($account) = @_;

   print "Getting index for account $account ";
   my $env = ArgGet("env") || "int";
   my $baseurl = 'https://correspondence-archive.' .$env. '.gainesville.infiniteenergy.com:443/services/CorMan/CorrespondenceArchive.WebApi/correspondence';
   my $queryparams = '?accountNumber='.$account.'&includePrivateCorrespondence=true&includeOldCorrespondence=true';
   my $params = ' -s';
   my $cmd = 'curl "' . $baseurl . $queryparams. '"' . $params;
   print "$cmd\n" if ArgIs("debug");

   my $result = `$cmd`;

   if ($?) {
      print "unable to get index for $account\n"; 
      return [];
   }
   my $index = decode_json($result);
   return $index;
}


sub DumpFile {
   my ($entry, $dir, $localIndex) = @_;

   $entry->{filename} = MakeFilename($entry);
   $entry->{filespec} = $dir . "\\" . $entry->{filename};

   print DumpRef($entry, ' ', 3) . "\n" if ArgIs("verbose");

   my $params = ' -s -o "' . $entry->{filespec} . '"';
   my $cmd = 'curl "' . $entry->{contentUri} . '"' . $params;
   print "   $cmd\n" if ArgIs("debug");
   my $result = `$cmd`;

   if (ExamineFile($entry)) {
      print "   Writing '$entry->{filespec}'\n";
      $STATS->{files}++;
      LogEntry($entry, $localIndex);
   } else {
      print "   Can't read $entry->{filename} : $entry->{error}\n";
      unlink($entry->{filespec});
      LogError($entry);
   }
}


# going this route because I'm having issues with LWP & https
#
sub ExamineFile {
   my ($entry) = @_;

   my $filespec = $entry->{filespec};
   my $size = (stat($filespec))[7];
   return 1 if $size > 10000;

   my $data = SlurpFile($filespec);
   my $info;
   eval {
      $info = decode_json($data);
   };
   return 1 if ($@);

   $entry->{error} = $info->{exceptionMessage};
   return 0;
}


sub MakeAccountDir {
   my ($account) = @_;

   my $dir = ArgGet() || "files";
   my ($sub) = $account =~ /^(.{3})/;

   mkdir $dir;
   mkdir $dir . "\\$sub";
   mkdir $dir . "\\$sub\\$account";
   return $dir. "\\$sub\\$account";
}


sub MakeFilename {
   my ($entry) = @_;

   my $type = $entry->{packetType};
   my $id   = $entry->{packetId};
   my $fmat = lc ($entry->{fileFormat} || "unknown");
   my $vals = $entry->{identifierValues};
   my $hasVals = scalar keys %{$vals};

   $fmat = "pdf" if $fmat =~ /other/i;

   if ($hasVals && $vals->{invoiceNumber}) {
      return $type . "-" . $vals->{invoiceNumber} . "." . $fmat;
   }
   if ($hasVals && $vals->{cmDocumentId}) {
      return $type . "-" . $vals->{cmDocumentId} . "." . $fmat;
   }
   return $type. "-" . $id . "." . $fmat;
}


sub GetServer {
   my $env = ArgGet("env") || "int";
   return $env =~ /^prod/i    ? "babyadept"      :
          $env =~ /^preprod/i ? "test-babyadept" :
                                "adepttrunk"     ;
}


sub ShowStats {
   my $delta    = (time() - $STATS->{start}) / 60;
   my $fileRate = sprintf("%.2f", $STATS->{files   } / $delta);
   my $acctRate = sprintf("%.2f", $STATS->{accounts} / $delta);
   my $guess    = sprintf("%.2f", ($STATS->{totalaccounts} / ($STATS->{accounts} / $delta)) / 1440);

   printf "[Processed: $STATS->{accounts} of $STATS->{totalaccounts} accounts ($acctRate /min), $STATS->{files} files ($fileRate /min),  Projection: $guess days]\n\n";
}


sub LogEntry {
   my ($entry, $localIndex) = @_;

   state $globalIndex;

   if (!$globalIndex) {
      my $dir = ArgGet() || "files";
      my $suffix = ArgGet("suffix") || "";
      open ($globalIndex, ">", "$dir\\index$suffix.csv");
      WriteIndexHeader($globalIndex) unless ArgIs("skipheaders");
   }
   WriteIndexEntry($globalIndex, $entry);
   WriteIndexEntry($localIndex, $entry);
}


sub LogError {
   my ($entry) = @_;

   state $globalError;

   if (!$globalError) {
      my $dir = ArgGet() || "files";
      my $suffix = ArgGet("suffix") || "";
      open ($globalError, ">", "$dir\\error$suffix.csv");
      WriteErrorHeader($globalError) unless ArgIs("skipheaders");
   }
   WriteErrorEntry($globalError, $entry);
}


sub WriteIndexHeader {
   my ($filehandle) = @_;

   printf $filehandle "accountNumber,"  .
                      "packetType,"     .
                      "filename,"       .
                      "archiveDate,"    .
                      "packetId,"       .
                      "fileFormat,"     .
                      "invoiceNumber,"  .
                      "cmDocumentId"    .
                      "\n"              ;
}


sub WriteIndexEntry {
   my ($filehandle, $entry) = @_;

   my $vals = $entry->{identifierValues};
   my $invoiceNumber = $vals ? $vals->{invoiceNumber} || "" : "";
   my $cmDocumentId  = $vals ? $vals->{cmDocumentId } || "" : "";

   print $filehandle "$entry->{accountNumber}," .
                     "$entry->{packetType},"    .
                     "$entry->{filename},"      .
                     "$entry->{archiveDate},"   .
                     "$entry->{packetId},"      .
                     "$entry->{fileFormat},"    .
                     "$invoiceNumber,"          .
                     "$cmDocumentId"            .
                     "\n"                       ;
}


sub WriteErrorHeader {
   my ($filehandle) = @_;

   printf $filehandle "accountNumber," .
                      "packetType,"    .
                      "filename,"      .
                      "error,"         .
                      "archiveDate,"   .
                      "packetId,"      .
                      "fileFormat,"    .
                      "invoiceNumber," .
                      "cmDocumentId"   .
                      "\n"             ;
}


sub WriteErrorEntry {
   my ($filehandle, $entry) = @_;

   my $vals = $entry->{identifierValues};
   my $invoiceNumber = $vals ? $vals->{invoiceNumber} || "" : "";
   my $cmDocumentId  = $vals ? $vals->{cmDocumentId } || "" : "";

   print $filehandle "$entry->{accountNumber}," .
                     "$entry->{packetType},"    .
                     "$entry->{filename},"      .
                     "$entry->{error},"         .
                     "$entry->{archiveDate},"   .
                     "$entry->{packetId},"      .
                     "$entry->{fileFormat},"    .
                     "$invoiceNumber,"          .
                     "$cmDocumentId"            .
                     "\n"                       ;
}


sub ListAccounts {
   my ($accounts) = @_;

   foreach my $account (@{$accounts}) {
      print $account, "\n";
   }
   print "(" . scalar @{$accounts} . " accounts)\n";
   exit(0);
}


__DATA__

[usage]
DumpTXCAFiles.pl  -  Download TX correspondence files to disk

USAGE:  DumpTXCAFiles.pl [options] outdir

WHERE:
   outdir ......... The root directory of the file tree

   [options] are 0 or more of:
      -env............ Set the environment (int|preprod|prod) default is int
      -account=#...... Only process this account
      -start=#........ Start at this account number
      -end=#.......... End before this account number
      -suffix=string.. Add a suffix on to the global csv filenames
      -skipheaders.... Don't write CSV header line
      -test=#......... Only dump files from this many accounts
      -debug.......... Show debug output
      -help .......... This help

EXAMPLES:
   DumpTXCAFiles.pl files
   DumpTXCAFiles.pl -start=2000000000 -end=2002000000 -skipheader files
   DumpTXCAFiles.pl -account=1000011639 files
   DumpTXCAFiles.pl -debug files
   DumpTXCAFiles.pl -env=preprod files
   DumpTXCAFiles.pl -env=prod -test=3 files

INFO:
   Directories are generated for each account to contain the account files.
   The dir structure is:
     [outdir]\[prefix]\[account#]
   where [prefix] is the first 3 digits of the account number.

   A global index.csv is created in [outdir]
   An account index.csv is generated for each account in the account dir.

[fini]
