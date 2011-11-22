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

$pgversion = $dbh->{pg_server_version};
$pgvstring = $dbh->selectall_arrayref('SELECT VERSION()')->[0][0];

print "Server version: $pgversion; $pgvstring\n";

$dbh->disconnect();