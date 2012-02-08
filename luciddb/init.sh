#!/bin/bash
# Licensed to DynamoBI Corporation (DynamoBI) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  DynamoBI licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

#   http:www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

osdist ()
{
    if [ -f "/etc/issue" ]; then
        echo "bash_exe=/bin/bash"

        if ( cat /etc/issue | grep -q -i ubuntu ) ; then
            echo "os=Ubuntu"
            echo "osver=`cat /etc/issue | grep -v '^[ \t]*$' | awk '{print $2;}'`"

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
            echo "osver=`cat /etc/issue | grep -v '^[ \t]*$' | awk '{print $7;}'`"

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
            if [ "$(uname -m)" = "x86_64" ]; then
                echo "os=RedHat_64"
            else
                echo "os=RedHat"
            fi

            echo "osver=`cat /etc/issue | grep -i 'red hat' | sed -e 's/^.*\(release [^ \t][^ \t]*\).*/\1/g' | awk '{print $2;}'`"

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

        elif ( cat /etc/issue | grep -q -i 'CentOS' ) ; then
            if [ "$(uname -m)" = "x86_64" ]; then
                echo "os=CentOS_64"
            else
                echo "os=CentOS"
            fi

            echo "osver=`cat /etc/issue | grep -i 'CentOS' | sed -e 's/^.*\(release [^ \t][^ \t]*\).*/\1/g' | awk '{print $2;}'`"

            if ( cat /proc/cpuinfo | grep '^flags' | tail -n 1 | grep -q -w ht ); then
                echo "ht=true"
            else
                echo "ht=false"
            fi
            cpus=`cat /proc/cpuinfo | grep '^processor' | wc -l`
            echo "cpus=$cpus"


        else
            echo "os=Unknown"
            echo "osver=Unknown"
            echo "ht=Unknown"
            echo "cpus=Unknown"
        fi
    else
        echo "bash_exe=$(cygpath -m -a /bin/bash)"

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

    buildarch=$(echo $(uname -o)/$(uname -m) | tr [/] '-' | tr [:upper:] [:lower:])
    echo "buildarch=${buildarch}"

    if ( 2>&1 java -version | grep -i -q hotspot ); then
        echo "jvm=hotspot"
    elif ( 2>&1 java -version | grep -i -q jrockit ); then
        echo "jvm=jrockit"
    else
        echo "jvm=unknown"
    fi
}

eval $(osdist)

if [[ "${os}" == RedHat* ]] ; then
    case "${osver}" in
      5.1) osver=5 ;;
    esac
fi

if [ "$1" = "export" ]; then  # for nightly bit
  echo "export build_os=${os}"
  echo "export build_osver=${osver}"
  echo "export build_cpus=${cpus}"
  echo "export build_jvm=${jvm}"
  echo "export build_arch=${buildarch}"
else
  echo "build.os=${os}"
  echo "build.osver=${osver}"
  echo "build.cpus=${cpus}"
  echo "build.jvm=${jvm}"
  echo "bash.exe=${bash_exe}"
  echo "build.arch=${buildarch}"
fi
