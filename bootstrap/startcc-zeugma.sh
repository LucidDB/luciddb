#!/bin/sh

./initcc-open.sh

export PATH=$PATH:/home/cruise/open/bootstrap/cruisecontrol-2.3.1/main/bin
export HOSTNAME

nice cruisecontrol.sh -configfile /home/cruise/open/bootstrap/config-zeugma.xml -port 8081
