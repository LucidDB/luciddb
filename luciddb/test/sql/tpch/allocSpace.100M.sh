#!/bin/sh

LUCIDDB_DIR=`pwd`/../../..
$LUCIDDB_DIR/lucidDbAllocFile \
    --append-pages=3000 $LUCIDDB_DIR/catalog/db.dat
