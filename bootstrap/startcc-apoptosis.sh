#!/bin/sh

./initcc-open.sh

export PATH=$PATH:/home/cruise/open/bootstrap/cruisecontrol-2.1.5/main/bin
export HOSTNAME
nice cruisecontrol.sh -configfile config-apoptosis.xml -port 8081
