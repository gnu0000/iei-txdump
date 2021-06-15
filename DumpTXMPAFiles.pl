#!perl
#
# This script gets all TX correspondence from MPA
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
      totalfiles => 0,
      contracts  => 0,
      goodfiles  => 0,
      badfiles   => 0,
      start      => time()
      };

MAIN:
   $| = 1;

   ArgBuild("*^env= *^start= *^end= *^suffix= *^list *^skipheaders *^debug *^verbose *^help");
   ArgParse(@ARGV) or die ArgGetError();
   Usage() if ArgIs("help") || !ArgIs();

   my $server = GetServer();
   print "Connecting to $server\n";
   my $db = MSConnect($server, "MPA2");

   my $rows = GetAttachmentRows($db);
   ListContracts($rows) if ArgIs("list");
   ProcessRows($db, $rows);

   print "Processed $STATS->{totalfiles} files in $STATS->{contracts} contracts.\n";
   exit(0);


sub GetAttachmentRows {
   my ($db) = @_;

   my $sql = 
      " select" .
      "    c.id as contractid," .
      "    q.id as quoteid," .
      "    af.id as attachmentid," .
      "    af.filename," .
      "    q.companyname," .
      "    q.contactname," .
      "    q.contactphone," .
      "    af.uploaded" .
      " from" .
      " 	mpa2.dbo.Contract c" .
      " 	join mpa2.dbo.Quote q on q.id = c.quoteId" .
      "    join mpa2.dbo.AttachmentFile af on af.contractId = c.id" .
      " where" .
      "    q.market = 4" .
      "    and af.fileexists = 1" .
      " order by c.id, af.uploaded";

   my $rows = FetchArray($db, $sql);
   
   if (ArgIsAny("start", "end")) {
      my @tmp = ();
      my $start = ArgGet("start") || 0;
      my $end   = ArgGet("end"  ) || 10000000000;
      foreach my $row ($rows) {
         my $contractId = $row->{contractid};
         next if $contractId < $start || $contractId >= $end;
         push (@tmp, $contractId);
      }
      $rows = \@tmp;
   }
   $STATS->{totalfiles} = scalar @{$rows};
   return $rows;
}


sub ProcessRows {
   my ($db, $rows) = @_;

   my ($contractid, $dir, $ieinum, $ldcnum) = ("", "", "", "");
   my $localIndex = undef;
   foreach my $row (@{$rows}) {
      if ($row->{contractid} ne $contractid) {
         close ($localIndex) if $localIndex;
         $contractid = $row->{contractid};
         ($ieinum, $ldcnum) = FindIDs($db, $row->{quoteid});

         $dir = MakeContractDir($contractid);
         $STATS->{contracts}++;
         open ($localIndex, ">", "$dir\\index.csv");
         WriteIndexHeader($localIndex);
      }
      PrepRow($row, $ieinum, $ldcnum);
      DumpFile($row, $dir, $localIndex);

      ShowStats() if !($STATS->{goodfiles} % 10);
   }
   close ($localIndex);
}


sub FindIDs {
   my ($db, $quoteid) = @_;

   my $sql =
      " select"                         .
      "    ldcAccountNumber as ldcnum," . 
      "    ieiAccountNumber as ieinum," . 
      "    companyname"                 .
      " from mpa2.dbo.ServicePoint"     .
      " where quoteid = $quoteid"       ;
   
   my $sps = FetchArray($db, $sql);

   my ($ieinum, $ldcnum) = ("", "");
   foreach my $sp (@{$sps}) {
      $ieinum = $sp->{ieinum} unless $ieinum;
      $ldcnum = $sp->{ldcnum} unless $ldcnum;
   }
   return ($ieinum, $ldcnum);
}


sub MakeContractDir {
   my ($contractId) = @_;

   my $dir = ArgGet() || "mpafiles";
   my ($sub) = $contractId =~ /^(.{2})/;

   mkdir $dir;
   mkdir $dir . "\\$sub";
   mkdir $dir . "\\$sub\\$contractId";
   return $dir. "\\$sub\\$contractId";
}



sub DumpFile {
   my ($row, $dir, $localIndex) = @_;

   $row->{filename} = MakeFilename($row);
   $row->{filespec} = $dir . "\\" . $row->{filename};

   print DumpRef($row, ' ', 3) . "\n" if ArgIs("verbose");

   my $env = ArgGet("env") || "int";
   my $url = "https://mpa." .$env. ".gainesville.infiniteenergy.com/api/attachments/$row->{attachmentid}";
   my $params = ' -s -o "' . $row->{filespec} . '"';
   my $cmd = 'curl "' . $url . '"' . $params;
   print "   $cmd\n" if ArgIs("debug");
   my $result = `$cmd`;

   if ($!) {
      $row->{error} = $!;
      print "   Can't read $row->{filename} : $row->{error}\n";
      unlink($row->{filespec});
      $STATS->{badfiles}++;
      LogError($row);
   } else {
      print "   Writing '$row->{filespec}'\n";
      $STATS->{goodfiles}++;
      LogEntry($row, $localIndex);
   }
}


sub ExamineFile {
   return 1;
}


sub MakeFilename {
   my ($row) = @_;

   return $row->{attachmentid} . "-" . $row->{filename};
}


sub PrepRow {
   my ($row, $ieinum, $ldcnum) = @_;

   $row->{contractid  } ||= "";
   $row->{quoteid     } ||= "";
   $row->{attachmentid} ||= "";
   $row->{filename    } ||= "";
   $row->{companyname } ||= "";
   $row->{contactname } ||= "";
   $row->{contactphone} ||= "";
   $row->{uploaded    } ||= "";
   $row->{ieinum      } = $ieinum || "";
   $row->{ldcnum      } = $ldcnum || "";

   $row->{companyname } =~ s/,//;
   $row->{contactname } =~ s/,//;

   return $row;
}


sub GetServer {
   my $env = ArgGet("env") || "int";
   return $env =~ /^prod/i    ? "babyadept"      :
          $env =~ /^preprod/i ? "test-babyadept" :
                                "adepttrunk"     ;
}


sub ShowStats {
   my $delta    = (time() - $STATS->{start}) / 60;
   my $fileRate = sprintf("%.2f", $STATS->{goodfiles} / $delta);
   my $acctRate = sprintf("%.2f", $STATS->{contracts} / $delta);
   my $guess    = sprintf("%.2f", ($STATS->{totalfiles} / ($STATS->{goodfiles} / $delta)) / 60);

   printf "[Processed: $STATS->{goodfiles} of $STATS->{totalfiles} files ($fileRate /min), $STATS->{contracts} contracts ($acctRate /min),  Projection: $guess hours]\n\n";
}


sub LogEntry {
   my ($row, $localIndex) = @_;

   state $globalIndex;

   if (!$globalIndex) {
      my $dir = ArgGet() || "files";
      my $suffix = ArgGet("suffix") || "";
      open ($globalIndex, ">", "$dir\\index$suffix.csv");
      WriteIndexHeader($globalIndex) unless ArgIs("skipheaders");
   }
   WriteIndexEntry($globalIndex, $row);
   WriteIndexEntry($localIndex, $row);
}


sub LogError {
   my ($row) = @_;

   state $globalError;

   if (!$globalError) {
      my $dir = ArgGet() || "files";
      my $suffix = ArgGet("suffix") || "";
      open ($globalError, ">", "$dir\\error$suffix.csv");
      WriteErrorHeader($globalError) unless ArgIs("skipheaders");
   }
   WriteErrorEntry($globalError, $row);
}


sub WriteIndexHeader {
   my ($filehandle) = @_;

   printf $filehandle "contractId,"   .
                      "quoteId,"      .
                      "ieiNumber,"    .
                      "ldcNumber,"    .
                      "attachmentId," .
                      "filename,"     .
                      "companyName,"  .
                      "contactName,"  .
                      "contactPhone," .
                      "uploaded"      .
                      "\n"            ;
}


sub WriteIndexEntry {
   my ($filehandle, $row) = @_;

   #print DumpRef($row, ' ', 3) . "\n" if ArgIs("debug");
   #print "debug contractid   :: $row->{contractid},"   . "\n" ;
   #print "debug quoteid      :: $row->{quoteid},"      . "\n" ;
   #print "debug ieinum       :: $row->{ieinum},"       . "\n" ;
   #print "debug ldcnum       :: $row->{ldcnum},"       . "\n" ;
   #print "debug attachmentid :: $row->{attachmentid}," . "\n" ;
   #print "debug filename     :: $row->{filename},"     . "\n" ;
   #print "debug companyname  :: $row->{companyname},"  . "\n" ;
   #print "debug contactname  :: $row->{contactname},"  . "\n" ;
   #print "debug contactphone :: $row->{contactphone}," . "\n" ;
   #print "debug uploaded     :: $row->{uploaded}"      . "\n" ;
   #print "\n"                    ;

   print $filehandle "$row->{contractid},"   .
                     "$row->{quoteid},"      .
                     "$row->{ieinum},"       .
                     "$row->{ldcnum},"       .
                     "$row->{attachmentid}," .
                     "$row->{filename},"     .
                     "$row->{companyname},"  .
                     "$row->{contactname},"  .
                     "$row->{contactphone}," .
                     "$row->{uploaded}"      .
                     "\n"                    ;
}


sub WriteErrorHeader {
   my ($filehandle) = @_;

   printf $filehandle "contractId,"   .
                      "quoteId,"      .
                      "ieiNumber,"    .
                      "ldcNumber,"    .
                      "attachmentId," .
                      "filename,"     .
                      "companyName,"  .
                      "contactName,"  .
                      "contactPhone," .
                      "error"      .
                      "\n"            ;
}


sub WriteErrorEntry {
   my ($filehandle, $row) = @_;

   print $filehandle "$row->{contractid},"   .
                     "$row->{quoteid},"      .
                     "$row->{ieinum},"       .
                     "$row->{lcdnum},"       .
                     "$row->{attachmentid}," .
                     "$row->{filename},"     .
                     "$row->{companyname},"  .
                     "$row->{contactname},"  .
                     "$row->{contactphone}," .
                     "$row->{error}"         .
                     "\n"                    ;
}


sub ListContracts {
   my ($rows) = @_;

   my ($contractid, $count) = ("", 0);
   foreach my $row (@{$rows}) {
      next if $row->{contractid} eq $contractid;
      $contractid = $row->{contractid};
      print "$contractid\n";
      $count++;
      }
   print "($count contracts)\n";
   print "(" . scalar @{$rows} . " attachments)\n";
   exit(0);
}


__DATA__

[usage]
DumpTXMPAFiles.pl  -  Download TX MPA attachments to disk

USAGE:  DumpTXFiles.pl [options] outdir

WHERE:
   outdir ......... The root directory of the file tree

   [options] are 0 or more of:
      -env............ Set the environment (int|preprod|prod) default is int
      -contract=#...... Only process this contract
      -start=#........ Start at this contract number
      -end=#.......... End before this contract number
      -suffix=string.. Add a suffix on to the global csv filenames
      -skipheaders.... Don't write CSV header line
      -test=#......... Only dump files from this many contracts
      -debug.......... Show debug output
      -help .......... This help

EXAMPLES:
   DumpTXFiles.pl mpaFiles
   DumpTXFiles.pl -start=400000 -end=500000 -skipheader mpaFiles
   DumpTXFiles.pl -contract=456545 mpaFiles
   DumpTXFiles.pl -debug mpaFiles
   DumpTXFiles.pl -env=preprod mpaFiles
   DumpTXFiles.pl -env=prod -test=3 mpaFiles

INFO:
   Directories are generated for each contract to contain the contract files.
   The dir structure is:
     [outdir]\[prefix]\[contract#]
   where [prefix] is the first 3 digits of the contract number.

   A global index.csv is created in [outdir]
   An contract index.csv is generated for each contract in the contract dir.

[fini]
