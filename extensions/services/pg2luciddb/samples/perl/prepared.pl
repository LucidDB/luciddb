#!perl

use DBI;
use DBD::Pg qw(:pg_types);
use DBI ':sql_types';

### change settings to your host with pg2luciddb:
my $dbname = "LOCALDB";
my $host = "localhost";
my $port = 9999;
my $dbuser = "sa";
my $dbpass = "";

$dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$host;port=$port", $dbuser, $dbpass, {AutoCommit => 0, RaiseError => 1, PrintError => 0});

print "Established connection to PG2LucidDB\n";

### get list of locations:
print "\nLOCATIONS table\n";
print "ID	NAME	MANAGER	SQUARE	ISFASTFOOD	ISCLOSED	DATEADDED	DATEOPENED\n\n";	

my $SQL = "SELECT * FROM PG2LUCIDDBTEST.LOCATION WHERE ID = ? ORDER BY ID";
my $sth = $dbh->prepare($SQL);

### execute with param:
my $rv = $sth->execute(2);

if (!defined $rv) 
{
    print "Error occured during query execution: " . $dbh->errstr . "\n";
    exit(0);
}

while (my @array = $sth->fetchrow_array()) 
{
    foreach $i (@array) {
       print "$i\t";
    }
    print "\n";
}

$sth->finish();

### execute with param:
$sth->execute(1);

while (my @array = $sth->fetchrow_array()) 
{
    foreach $i (@array) {
       print "$i\t";
    }
    print "\n";
}

$sth->finish();

### fetch with 2 parameters:

print "\nSKU table\n";
print "ID	NAME	PRICE	CURRENCY\n\n";	


$dbh->do("SET SCHEMA 'PG2LUCIDDBTEST'");
$SQL = "SELECT ID, NAME, PRICE, CURRENCY FROM SKU WHERE CURRENCY = ? AND PRICE >= ? ORDER BY NAME ASC";
$sth = $dbh->prepare($SQL);

### execute:
$sth->execute("RUR", 175.50);

while (my @array = $sth->fetchrow_array()) 
{
    foreach $i (@array) {
       print "$i\t";
    }
    print "\n";
}

$sth->finish();

### execute:
$sth->execute("EUR", 0.1);

while (my @array = $sth->fetchrow_array()) 
{
    foreach $i (@array) {
       print "$i\t";
    }
    print "\n";
}

$sth->finish();


$dbh->disconnect();