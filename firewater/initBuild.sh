#!/bin/bash
# $Id$
# Copyright (C) 2009-2009 John V. Sichi
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the Free
# Software Foundation; either version 2 of the License, or (at your option)
# any later version approved by The Eigenbase Project.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#  
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
set -e

cd ../farrago
ant restoreCatalog
cd ../luciddb
ant restoreCatalog
cd ../firewater
ant createPlugin
cd ../farrago
ant restoreCatalog
cd ../firewater
ant -Dtest.sessionfactory=class:net.sf.farrago.defimpl.FarragoDefaultSessionFactory -Dfileset.unitsql=initsql/installMetamodel.sql -Djunit.class=com.lucidera.luciddb.test.LucidDbSqlTest test
# junitSingle initsql/installMetamodel.sql
# cd ../luciddb
# sed -i -e 's/VIEW/AUTO_VIEW/g' catalog/ReposStorage.properties
cd ../firewater
junitSingle initsql/installSystemObjects.sql
# cd ../luciddb
# sed -i -e 's/AUTO_VIEW/VIEW/g' catalog/ReposStorage.properties
cd ../firewater
ant backupCatalog
