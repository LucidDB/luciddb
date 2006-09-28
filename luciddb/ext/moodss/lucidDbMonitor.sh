#!/bin/bash
# $Id$
#
# Run the LucidDB graphical monitor
#
# Usage:
#
#     lucidDbMonitor.sh [ url ] [ username ] [ password ]
#
# Defaults:
#
# url:  jdbc:luciddb:rmi://localhost
# user:  sa
# password: <blank>
#
# Example:
#
#     lucidDbMonitor.sh jdbc:luciddb:rmi://beefstew chef boyardee
#

java -cp LucidDbMonitor.jar:../../plugin/LucidDbClient.jar LucidDbMonitor $*
