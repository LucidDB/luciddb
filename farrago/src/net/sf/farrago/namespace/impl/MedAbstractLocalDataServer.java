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

import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.*;


/**
 * MedAbstractLocalDataServer is an abstract base class for implementations of
 * the {@link FarragoMedLocalDataServer} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractLocalDataServer
    extends MedAbstractDataServer
    implements FarragoMedLocalDataServer
{
    //~ Instance fields --------------------------------------------------------

    private FennelDbHandle fennelDbHandle;

    //~ Constructors -----------------------------------------------------------

    protected MedAbstractLocalDataServer(
        String serverMofId,
        Properties props)
    {
        super(serverMofId, props);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the Fennel database handle to use for accessing local storage
     */
    public FennelDbHandle getFennelDbHandle()
    {
        return fennelDbHandle;
    }

    // implement FarragoMedDataServer
    public FarragoMedNameDirectory getNameDirectory()
        throws SQLException
    {
        return null;
    }

    // implement FarragoMedLocalDataServer
    public void setFennelDbHandle(FennelDbHandle fennelDbHandle)
    {
        this.fennelDbHandle = fennelDbHandle;
    }

    // implement FarragoMedLocalDataServer
    public void validateTableDefinition(
        FemLocalTable table,
        FemLocalIndex generatedPrimaryKeyIndex)
        throws SQLException
    {
        // by default, no special validation rules
    }

    // implement FarragoMedLocalDataServer
    public void validateTableDefinition(
        FemLocalTable table,
        FemLocalIndex generatedPrimaryKeyIndex,
        boolean creation)
        throws SQLException
    {
        validateTableDefinition(table, generatedPrimaryKeyIndex);
    }

    // implement FarragoMedLocalDataServer
    public boolean supportsAlterTableAddColumn()
    {
        // Assume not; subclasses have to override this
        // to enable support.
        return false;
    }
}

// End MedAbstractLocalDataServer.java
