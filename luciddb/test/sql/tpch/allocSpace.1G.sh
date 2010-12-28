#!/bin/sh

LUCIDDB_DIR=`pwd`/../../..
$LUCIDDB_DIR/lucidDbAllocFile \
    --append-pages=33000 $LUCIDDB_DIR/catalog/db.dat
$LUCIDDB_DIR/lucidDbAllocFile \
    --append-pages=4000 $LUCIDDB_DIR/catalog/temp.dat
