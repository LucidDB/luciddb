#!/bin/sh

export ANT_HOME=`pwd`/`dirname $0`/ant

$ANT_HOME/bin/ant $*
