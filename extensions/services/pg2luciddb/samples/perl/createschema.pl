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

### create schema:
$dbh->do("CREATE SCHEMA PG2LUCIDDBTEST");

### set schema:
$dbh->do("SET SCHEMA 'PG2LUCIDDBTEST'");

print "PG2LUCIDDBTEST schema has been successfully created\n";

### create table Location:

$SQL = q{
CREATE TABLE LOCATION (
  Id integer not null primary key,
  Name varchar(255) not null,
  Manager varchar(128),
  Square float not null,
  IsFastFood boolean not null,
  IsClosed boolean not null,
  DateAdded timestamp not null,
  DateOpened date not null  
)
};

$dbh->do($SQL);

### append sample data:
$SQL = "INSERT INTO LOCATION VALUES (1, 'Location #1', 'Manager for Location #1', 251.8, false, false, current_timestamp, APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-07-14'))";
$count = $dbh->do($SQL);

### unicode test:
$SQL = "INSERT INTO LOCATION VALUES (2, 'Ресторан #2', NULL, 58.4, true, false, current_timestamp, APPLIB.CHAR_TO_DATE('yyyy-M-d', '2008-02-01'))";
$count = $count + $dbh->do($SQL);

print "Table LOCATION has been successfully created, $count rows added\n";

### create table SKU:

$SQL = q{
CREATE TABLE SKU (
   Id smallint not null primary key,
   Name varchar(128) not null,
   Price numeric(6, 2) not null,
   Currency char(3) not null
)
};

$dbh->do($SQL);

### insert data using prepared statement:
$SQL = "INSERT INTO SKU VALUES (?, ?, ?, ?)";
my $sth = $dbh->prepare($SQL);

$count = $sth->execute(1, "Espresso Dopio", 185, "RUR"); 
$count = $count + $sth->execute(2, "Latte Medio", 175.50, "RUR"); 
$count = $count + $sth->execute(3, "Hamburger", 3.75, "USD"); 
$count = $count + $sth->execute(4, "Coca-cola", 0.8, "EUR"); 

print "Table SKU has been successfully created, $count rows added\n";

### create table SALES:

$SQL = q{
CREATE TABLE SALES_FACT (
   TransactionId bigint not null primary key,
   LocationId integer not null,
   SKUId smallint not null,
   TransactionTime time not null,
   TransactionDate date not null,
   Comments varchar(128)
)
};

$dbh->do($SQL);

### append sample data:
$SQL = "INSERT INTO SALES_FACT VALUES (1, 1, 1, APPLIB.CHAR_TO_TIME('kk:mm', '08:10'), APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-23'), NULL)";
$count = $dbh->do($SQL);

$SQL = "INSERT INTO SALES_FACT VALUES (2, 1, 3, APPLIB.CHAR_TO_TIME('kk:mm', '08:10'), APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-23'), NULL)";
$count = $count + $dbh->do($SQL);

$SQL = "INSERT INTO SALES_FACT VALUES (3, 2, 4, APPLIB.CHAR_TO_TIME('kk:mm', '13:15'), APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-24'), 'Promo')";
$count = $count + $dbh->do($SQL);
 
print "Table SALES_FACT has been successfully created, $count rows added\n"; 


$dbh->disconnect();