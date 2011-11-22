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

### get list of locations:
print "\nLOCATIONS table\n";
print "ID	NAME	MANAGER	SQUARE	ISFASTFOOD	ISCLOSED	DATEADDED	DATEOPENED\n\n";	

my $SQL = "SELECT * FROM PG2LUCIDDBTEST.LOCATION ORDER BY ID";

for (@{$dbh->selectall_arrayref($SQL)}) 
{
     print $_->[0] . "\t" . $_->[1] . "\t" . $_->[2] . "\t" . $_->[3] . "\t" . $_->[4] . "\t" . $_->[5] . "\t" . $_->[6] . "\t" . $_->[7] . "\n";
}

### get list of SKUs:
print "\n\nSKU table\n";
print "ID	NAME	PRICE	CURRENCY\n\n";

$SQL = "SELECT ID, NAME, PRICE, CURRENCY FROM PG2LUCIDDBTEST.SKU ORDER BY ID";

for (@{$dbh->selectall_arrayref($SQL)}) 
{
     print $_->[0] . "\t" . $_->[1] . "\t" . $_->[2] . "\t" . $_->[3] . "\n";
}

### get sales fact:
print "\n\nSALES_FACT for the date 2009-09-23\n";
print "TRANSACTION_ID	LOCATION	SKU	TIME	DATE\n\n";
$dbh->do("SET SCHEMA 'PG2LUCIDDBTEST'");

$SQL = "SELECT T1.TRANSACTIONID, T2.NAME, T3.NAME, T1.TRANSACTIONTIME, T1.TRANSACTIONDATE FROM SALES_FACT T1 INNER JOIN LOCATION T2 ON T2.ID = T1.LOCATIONID INNER JOIN SKU T3 ON T3.ID = T1.SKUID WHERE T1.TRANSACTIONDATE = APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-23') ORDER BY T1.TRANSACTIONID";

for (@{$dbh->selectall_arrayref($SQL)}) 
{
     print $_->[0] . "\t" . $_->[1] . "\t" . $_->[2] . "\t" . $_->[3] . "\t" . $_->[4] . "\n";
}

$dbh->disconnect();
