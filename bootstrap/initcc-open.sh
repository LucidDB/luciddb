#!/bin/sh


if [ "!" -d farrago ]; then
    mkdir farrago
fi

if [ "!" -d fennel ]; then
    mkdir fennel
fi

if [ "!" -d saffron ]; then
    mkdir saffron
fi

if [ "!" -d logs ]; then
    mkdir logs
    mkdir logs/aspen
    mkdir logs/farrago
    mkdir logs/fennel
    mkdir logs/saffron
fi

if [ "!" -d artifacts ]; then
    mkdir artifacts
    mkdir artifacts/farrago
    mkdir artifacts/fennel
    mkdir artifacts/saffron
fi

if [ "!" -d build ]; then
    mkdir build
    mkdir build/farrago
    mkdir build/fennel
    mkdir build/saffron
    mkdir build/thirdparty
fi

if [ "!" -d ant ]; then
    echo "Cruise Control requires an installed copy of ant in its working directory."
    echo "The best version to use is the one stored in //open/dev/thirdparty"
    exit 1;
fi

