#!/bin/sh

export configSuffix=-win32
./initcc-open.sh

export PATH=$PATH:/cruise/open/bootstrap/cruisecontrol-2.1.5/main/bin
export HOSTNAME
nice cruisecontrol.sh -configfile config-stilton.xml -port 8080
