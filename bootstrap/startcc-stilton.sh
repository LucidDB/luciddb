#!/bin/sh

export configSuffix=-win32
./initcc-open.sh

export PATH=$PATH:/cruise/bootstrap/cruisecontrol-2.1.5/main/bin
export HOSTNAME
export JAVA_HOME=d:\\j2sdk1.4.2_03
cmd /c cruisecontrol.bat -configfile config-stilton.xml -port 8080
