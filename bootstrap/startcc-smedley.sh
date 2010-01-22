#!/bin/sh

./initcc-open.sh

if [ "!" -d aspen ]; then
    mkdir aspen
fi

if [ "!" -d logs/aspen ]; then
    mkdir logs/aspen
fi

if [ "!" -d artifacts/aspen ]; then
    mkdir artifacts/aspen
fi

if [ "!" -d build/aspen ]; then
    mkdir build/aspen
fi

# can't specify this in config.xml...
export P4PASSWD=$CRUISE_P4PASSWD

cruisecontrol.sh -configfile config-smedley.xml -port 8080
