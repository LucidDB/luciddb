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
set -e

cd ../farrago
ant restoreCatalog
cd ../luciddb
ant restoreCatalog
cd ../firewater
ant createPlugin
ant initFiles
cd ../farrago
ant restoreCatalog
cd ../firewater
ant -Dtest.sessionfactory=class:net.sf.farrago.defimpl.FarragoDefaultSessionFactory -Dfileset.unitsql=initsql/installMetamodel.sql -Djunit.class=com.lucidera.luciddb.test.LucidDbSqlTest test
# junitSingle initsql/installMetamodel.sql
# cd ../luciddb
# sed -i -e 's/VIEW/AUTO_VIEW/g' catalog/ReposStorage.properties
cd ../firewater
./junitSingle initsql/installSystemObjects.sql
# cd ../luciddb
# sed -i -e 's/AUTO_VIEW/VIEW/g' catalog/ReposStorage.properties
# cd ../firewater
ant backupCatalog
