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

# Set LOGDIR to a sensible default.
if [ ! "$WEBDIR" ]; then
   export WEBDIR=$HOME/web
fi

export LOGDIR=$WEBDIR/logs

if [ ! -d "$LOGDIR" ]; then
   echo "Variable LOGDIR is not set or is not valid directory."
   exit 1
fi

export ARTDIR=$WEBDIR/artifacts

if [ ! -d "$ARTDIR" ]; then
   echo "Variable ARTDIR is not set or is not valid directory."
   exit 1
fi

debug=false

# Find all files more than 30 days old and delete them
find $LOGDIR -maxdepth 2 -daystart -mtime +30 \
 -not -name currentbuildstatus.txt \
 -not -name latest.xml \
 -type f \
 -exec rm {} ";" 

# Find all files more than 13 days old and compress them.
find $LOGDIR -maxdepth 2 -daystart -mtime +13 \
 -not -name "*.bz2" \
 -not -name currentbuildstatus.txt \
 -not -name latest.xml \
 -type f \
 -exec bzip2 {} ";" 

# If the directories contain no .xml files (because the last build was more
# than 13 days ago), decompress the most recent .xml.bz2
for i in $(find $LOGDIR/* -type d -maxdepth 0); do
   if [ "$(echo $i/*.xml)" = "$i/latest.xml" \
         -a "$(echo $i/*.xml.bz2)" != "$i/*.xml.bz2" ]; then
      mostRecent="$(ls $i/*.xml.bz2 | tail -1)"
      if [ -f "$mostRecent" ]; then
         $debug && echo "There are no .xml files in $i; decompressing the most recent .xml.bz2, $mostRecent"
         bzip2 -d "$mostRecent"
      fi
   fi

   rm -f $i/latest.xml
   # redirect stderr if file doesn't exist.
   mostRecentXml="$(ls $i/*.xml 2>/dev/null | tail -1)" 
   if [ "$mostRecentXml" != "" ]; then
       ln -s $mostRecentXml $i/latest.xml
   fi

done

for i in $(find $ARTDIR/* -type d -maxdepth 0); do
   rm -f $i/latest
   mostRecentDir="$(ls -d $(find $i/* -type d -maxdepth 0) | tail -1)"
   ln -s $mostRecentDir $i/latest
done

# End

