#!/bin/sh

LUCIDDB_DIR=`pwd`/../../..
$LUCIDDB_DIR/lucidDbAllocFile \
    --append-pages=352000 $LUCIDDB_DIR/catalog/db.dat
$LUCIDDB_DIR/lucidDbAllocFile \
    --append-pages=47000 $LUCIDDB_DIR/catalog/temp.dat
