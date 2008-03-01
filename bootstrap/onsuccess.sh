#!/bin/sh
BUILDNAME=$1
LINKNAME=$2
LINKINFO=$LINKNAME-info.txt

# called by CruiseControl with current directory set to ~/work
cd artifacts/$BUILDNAME
ANS=`find -name "SQLstream-*.bin" | sort -r | head -n 1`
INSTALLER=${ANS:2}

# CC artifactspublisher drops permissions. Make timestamped version executable.
chmod 555 $INSTALLER

# create link to installer and text file with the link target 
# so scp clients can determine what they've downloaded
rm $LINKINFO
ln -sf $INSTALLER $LINKNAME
echo $INSTALLER >$LINKINFO

# debugging info
#echo ----- $BUILDNAME ----- >>~/work/onsuccess.out.txt
#ls -ld $INSTALLER $LINKNAME >>~/work/onsuccess.out.txt
