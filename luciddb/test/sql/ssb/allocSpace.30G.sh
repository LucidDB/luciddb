#!/bin/sh

LUCIDDB_DIR=`pwd`/../../..
$LUCIDDB_DIR/lucidDbAllocFile \
    --append-pages=1056000 $LUCIDDB_DIR/catalog/db.dat
$LUCIDDB_DIR/lucidDbAllocFile \
    --append-pages=165000 $LUCIDDB_DIR/catalog/temp.dat
