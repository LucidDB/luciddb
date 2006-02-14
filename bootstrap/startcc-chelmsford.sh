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

cruisecontrol.sh -configfile config-chelmsford.xml -port 8080
