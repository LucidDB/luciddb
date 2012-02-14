/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.namespace.impl;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.namespace.*;


/**
 * MedAbstractNameDirectory is an abstract base class for implementations of the
 * {@link FarragoMedNameDirectory} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractNameDirectory
    extends MedAbstractBase
    implements FarragoMedNameDirectory
{
    //~ Methods ----------------------------------------------------------------

    // implement FarragoMedNameDirectory
    public FarragoMedNameDirectory lookupSubdirectory(String foreignName)
        throws SQLException
    {
        return null;
    }

    // implement FarragoMedNameDirectory
    public boolean queryMetadata(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink)
        throws SQLException
    {
        return false;
    }

    // implement FarragoMedNameDirectory
    public FemBaseColumnSet newImportedColumnSet(
        FarragoRepos repos,
        String tableName)
    {
        // By default, all imported objects are treated as foreign tables
        FemBaseColumnSet newColumnSet = repos.newFemForeignTable();
        newColumnSet.setModality(ModalityTypeEnum.MODALITYTYPE_RELATIONAL);
        return newColumnSet;
    }
}

// End MedAbstractNameDirectory.java
