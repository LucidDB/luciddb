#!/bin/sh

./initcc-open.sh

#export PATH=$PATH:/home/schoi/cruisecontrol-2.1.5/main/bin
export PATH=$PATH:/home/schoi/cc231/cruisecontrol-2.3.1/main/bin
export HOSTNAME

nice cruisecontrol.sh -configfile /home/schoi/open/bootstrap/config-chikai.xml -port 8081
