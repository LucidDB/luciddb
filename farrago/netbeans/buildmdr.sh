# /bin/bash
# $Id$

export CVSROOT=:pserver:anoncvs@cvs.netbeans.org:/cvs
cvs login <<EOF
EOF
cvs -z 6 co standard_nowww mdr_nowww xtest junit
cd nbbuild
ant
cd ../core/naming
ant
cd ../../openidex/looks
ant
cd ../../xtest
ant
cd ../junit
ant
cd ../mdr
ant
cd extras/mdrant
ant
cd ../../xmidiffs
wget http://www.omg.org/cgi-bin/apps/doc?ad/01-02-15.xml
cd ../extras/uml2mof
ant
