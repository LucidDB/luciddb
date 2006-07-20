#!/bin/bash

osdist ()
{
    if [ -f "/etc/issue" ]; then
        if ( cat /etc/issue | grep -q -i ubuntu ) ; then
            echo "os=Ubuntu"
            echo "osver=`cat /etc/issue | grep -v '^[    ]*$' | awk '{print $2;}'`"

            if ( cat /proc/cpuinfo | grep '^flags' | tail -n 1 | grep -q -w ht ); then
                echo "ht=true"
                cpus=`cat /proc/cpuinfo | grep '^processor' | wc -l`
                cpus=`expr $cpus \/ 2`
                echo "cpus=$cpus"
            else
                echo "ht=false"
                cpus=`cat /proc/cpuinfo | grep '^processor' | wc -l`
                echo "cpus=$cpus"
            fi

        elif ( cat /etc/issue | grep -q -i suse ) ; then
            echo "os=SUSE"
            echo "osver=`cat /etc/issue | grep -v '^[    ]*$' | awk '{print $7;}'`"

            if ( cat /proc/cpuinfo | grep '^flags' | tail -n 1 | grep -q -w ht ); then
                echo "ht=true"
                cpus=`cat /proc/cpuinfo | grep '^processor' | wc -l`
                cpus=`expr $cpus \/ 2`
                echo "cpus=$cpus"
            else
                echo "ht=false"
                cpus=`cat /proc/cpuinfo | grep '^processor' | wc -l`
                echo "cpus=$cpus"
            fi


        elif ( cat /etc/issue | grep -q -i 'red hat' ) ; then
            echo "os=RedHat"
            echo "osver=`cat /etc/issue | grep -i 'red hat' | sed -e 's/^.*\(release [^     ][  ]*\)/\1/g' | awk '{print $2;}'`"

            if ( cat /proc/cpuinfo | grep '^flags' | tail -n 1 | grep -q -w ht ); then
                echo "ht=true"
                cpus=`cat /proc/cpuinfo | grep '^processor' | wc -l`
                cpus=`expr $cpus \/ 2`
                echo "cpus=$cpus"
            else
                echo "ht=false"
                cpus=`cat /proc/cpuinfo | grep '^processor' | wc -l`
                echo "cpus=$cpus"
            fi


        else
            echo "os=Unknown"
            echo "osver=Unknown"
            echo "ht=Unknown"
            echo "cpus=Unknown"
        fi
    else
            echo "os=Win"
            #osver=`cat /proc/version | awk '{print $2;}' | sed -e 's/(/\\\\(/g' -e 's/)/\\\\)/g'`
            osver=`cat /proc/version | cut -d'-' -f2 | awk '{print $1;}'`
            if [ "$osver" = "5.1" ]; then
                echo osver="XP"
            elif [ "$osver" = "5.2" ]; then
                echo osver="2003"
            else
                echo osver="$Unknown"
            fi

            if ( cat /proc/cpuinfo | grep '^flags' | tail -n 1 | grep -q -w ht ); then
                echo "ht=true"
                cpus=`cat /proc/cpuinfo | grep '^processor' | wc -l`
                cpus=`expr $cpus \/ 2`
                echo "cpus=$cpus"
            else
                echo "ht=false"
                cpus=`cat /proc/cpuinfo | grep '^processor' | wc -l`
                echo "cpus=$cpus"
            fi
    fi

    if ( 2>&1 java -version | grep -i -q hotspot ); then
        echo "jvm=hotspot"
    elif ( 2>&1 java -version | grep -i -q jrockit ); then
        echo "jvm=jrockit"
    else
        echo "jvm=unknown"
    fi
}

eval $(osdist)

echo "build.os=${os}"
echo "build.osver=${osver}"
echo "build.cpus=${cpus}"
echo "build.jvm=${jvm}"
