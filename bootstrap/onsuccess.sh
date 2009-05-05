#!/bin/sh
BUILDNAME=$1
LINKNAME=$2
LINKINFO=$LINKNAME-info.txt
MD5INFO=$LINKNAME-md5.txt

# called by CruiseControl with current directory set to ~/work
cd artifacts/$BUILDNAME
ANS=`find -name "SQLstream-*.bin" | sort -r | head -n 1`
INSTALLER=${ANS:2}

# CC artifactspublisher copies stale link, info, and md5 to ~/web/artifacts. Delete them.
rm -f ~/web/artifacts/$BUILDNAME/$LINKNAME*

# CC artifactspublisher drops permissions. Make timestamped version executable.
chmod 755 $INSTALLER

# create link to installer and text file with the link target 
# so scp clients can determine what they've downloaded
rm -f $LINKINFO
ln -sf $INSTALLER $LINKNAME
echo $INSTALLER >$LINKINFO

# create MD5 signature file
rm -f $MD5INFO
DIR=$PWD
(cd `dirname $INSTALLER`; md5sum `basename $INSTALLER` >$DIR/$MD5INFO)

# debugging info
#echo ----- $BUILDNAME ----- >>~/work/onsuccess.out.txt
#ls -ld $INSTALLER $LINKNAME >>~/work/onsuccess.out.txt
