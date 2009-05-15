#!/bin/sh

./initcc-open.sh

if [ "!" -d aspen ]; then
    mkdir aspen
fi

if [ "!" -d logs/aspen ]; then
    mkdir logs/aspen
fi

if [ "!" -d logs/aspen-opto ]; then
    mkdir logs/aspen-opto
fi

if [ "!" -d artifacts/aspen ]; then
    mkdir artifacts/aspen
fi

if [ "!" -d artifacts/aspen-opto ]; then
    mkdir artifacts/aspen-opto
fi

if [ "!" -d build/aspen ]; then
    mkdir build/aspen
fi

cruisecontrol.sh -configfile /home/cruise/work/config-chelmsford.xml -port 8080
