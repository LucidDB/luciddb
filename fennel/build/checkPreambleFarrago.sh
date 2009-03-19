#!/bin/bash
# $Id$
# Check the preamble of farrago files.

set -e

fennelDir=$(dirname $0)/..
farragoDir=$(dirname $0)/../../farrago

# NOTE jvs 16-Nov-2005:  I am disabling this because (a)
# the assumption about the Perforce mapping for //open/util relative
# to fennel is unwarranted (it's not required to have this mapped,
# and it isn't even there after unpacking a src distribution tarball,
# and once a machine has multiple branches on it, it gets more
# complicated) and (b) the check should be done as part of
# the util build, not part of the main Eigenbase build.
# resgenDir=$(dirname $0)/../../util/resgen

# Check preambles of all Java files. Which copyright notices are
# required depends upon the zone of the file:
# * LucidEra: Files under farrago/src/com/lucidera must have
#   Eigenbase and LucidEra copyright notices.
# * SQLstream: Files under farrago/src/com/disruptivetech must
#   have Eigenbase and SQLstream copyright notices.
# * Eigenbase: All other files must have Eigenbase, LucidEra and
#   SQLstream copyright notices.

# NOTE jvs 16-Nov-2005:  I removed $resgenDir/src from the find directory
# list below for the reasons given above.

/usr/bin/find $farragoDir/src -name \*.java |
grep -v -F \
'farrago/src/net/sf/farrago/FarragoMetadataFactory.java
farrago/src/net/sf/farrago/FarragoMetadataFactoryImpl.java
farrago/src/net/sf/farrago/parser/impl/
farrago/src/net/sf/farrago/resource/FarragoResource.java
farrago/src/net/sf/farrago/resource/FarragoResource_en_US.java
farrago/src/net/sf/farrago/resource/FarragoInternalQuery.java
farrago/src/net/sf/farrago/resource/FarragoInternalQuery_en_US.java
farrago/src/net/sf/farrago/test/FarragoSqlTestWrapper.java
farrago/src/com/lucidera/lurql/parser/
farrago/src/org/eigenbase/resource/EigenbaseResource.java
farrago/src/org/eigenbase/resource/EigenbaseResource_en_US.java
farrago/src/org/eigenbase/sql/parser/impl/
util/resgen/src/org/eigenbase/resgen/ResourceDef.java
util/resgen/src/org/eigenbase/xom/MetaDef.java' |
(
exitCode=0;
while read filename
do
    zone=eigenbase
    component=farrago
    case "$filename" in
    */farrago/src/com/lucidera/*) 
        zone=lucidera
        ;;
    */farrago/src/com/disruptivetech/*) 
        zone=disruptivetech
        ;;
    */farrago/src/org/eigenbase/util/property/*) 
        component=farrago-eigenbase-lgpl
        ;;
    */farrago/src/org/eigenbase/*) 
        component=farrago-eigenbase 
        ;;
    */org/eigenbase/resgen/*) 
        component=resgen
        ;;
    */org/eigenbase/xom/*) 
        component=xom
        ;;
    *) ;;
    esac
    # Check this file. If there is an error, set the status code, but
    # carry on looking for errors in other files.
    $fennelDir/build/checkPreamble.sh -${component} -${zone} "${filename}" || exitCode=1
done;
exit $exitCode
)

# End checkPreambleFarrago.sh
