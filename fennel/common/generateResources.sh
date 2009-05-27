#!/bin/bash
# $Id$

if [ "!" -z "$ANT_HOME" ]; then
    ant -f buildResources.xml execute.resgen;
else
    echo
    echo "In order to regenerate FennelResource code, Apache Ant must be"
    echo "installed, the ANT_HOME environment variable set, and the Eigenbase"
    echo "Resource Generator (ResGen) JAR files (eigenbase-resgen.jar and"
    echo "eigenbase-xom.jar) must be in your CLASSPATH environment variable."
    echo
    exit 1
fi
