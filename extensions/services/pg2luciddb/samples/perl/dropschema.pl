#!perl

use DBI;
use DBD::Pg;

### change settings to your host with pg2luciddb:
my $dbname = "LOCALDB";
my $host = "localhost";
my $port = 9999;
my $dbuser = "sa";
my $dbpass = "";

$dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$host;port=$port", $dbuser, $dbpass, {AutoCommit => 0, RaiseError => 1, PrintError => 0});

print "Established connection to PG2LucidDB\n";

### drop schema:
$dbh->do("DROP SCHEMA PG2LUCIDDBTEST CASCADE");

print "PG2LUCIDDBTEST schema has been successfully deleted\n";

$dbh->disconnect();
