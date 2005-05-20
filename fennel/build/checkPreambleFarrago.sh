#!/bin/bash
# $Id$
# Check the preamble of farrago files.

set -e

fennelDir=$(dirname $0)/..
farragoDir=$(dirname $0)/../../farrago

# Check preambles of all Java files. Which copyright notices are
# required depends upon the zone of the file:
# * LucidEra: Files under farrago/src/com/lucidera must have
#   Eigenbase and LucidEra copyright notices.
# * Disruptive Tech: Files under farrago/src/com/disruptivetech must
#   have Eigenbase and Disruptive Tech copyright notices.
# * Eigenbase: All other files must have Eigenbase, LucidEra and
#   Disruptive Tech copyright notices.

/usr/bin/find $farragoDir/src -name \*.java |
grep -v -F \
'farrago/src/net/sf/farrago/FarragoMetadataFactory.java
farrago/src/net/sf/farrago/parser/impl/
farrago/src/net/sf/farrago/resource/FarragoResource.java
farrago/src/net/sf/farrago/resource/FarragoResource_en_US.java
farrago/src/org/eigenbase/resource/EigenbaseResource.java
farrago/src/org/eigenbase/resource/EigenbaseResource_en_US.java
farrago/src/org/eigenbase/sql/parser/impl/' |
(
exitCode=0;
while read filename
do
    zone=eigenbase
    component=farrago
    case "$filename" in
    */farrago/src/com/lucidera/*) zone=lucidera ;;
    */farrago/src/com/disruptivetech/*) zone=disruptivetech ;;
    */farrago/src/org/eigenbase/*) component=farrago-eigenbase ;;
    *) ;;
    esac
    # Check this file. If there is an error, set the status code, but
    # carry on looking for errors in other files.
    $fennelDir/build/checkPreamble.sh -${component} -${zone} "${filename}" || exitCode=1
done;
exit $exitCode
)

# End checkPreambleFarrago.sh
