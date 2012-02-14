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
package net.sf.farrago.namespace.mock;

import java.sql.*;

import java.util.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.type.*;


/**
 * MedMockNameDirectory provides a mock implementation of the {@link
 * FarragoMedNameDirectory} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockNameDirectory
    extends MedAbstractNameDirectory
{
    //~ Static fields/initializers ---------------------------------------------

    static final String COLUMN_NAME = "MOCK_COLUMN";

    //~ Instance fields --------------------------------------------------------

    final MedMockDataServer server;

    String scope;

    //~ Constructors -----------------------------------------------------------

    MedMockNameDirectory(MedMockDataServer server, String scope)
    {
        this.server = server;
        this.scope = scope;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoMedNameDirectory
    public FarragoMedColumnSet lookupColumnSet(
        FarragoTypeFactory typeFactory,
        String foreignName,
        String [] localName)
        throws SQLException
    {
        if (!scope.equals(FarragoMedMetadataQuery.OTN_TABLE)) {
            return null;
        }
        if (!foreignName.equals(server.getForeignTableName())) {
            return null;
        }

        return server.newColumnSet(
            localName,
            server.getProperties(),
            typeFactory,
            server.createMockRowType(typeFactory),
            Collections.EMPTY_MAP);
    }

    // implement FarragoMedNameDirectory
    public FarragoMedNameDirectory lookupSubdirectory(String foreignName)
        throws SQLException
    {
        if (scope.equals(FarragoMedMetadataQuery.OTN_SCHEMA)) {
            if (foreignName.equals(server.getForeignSchemaName())) {
                return new MedMockNameDirectory(
                    server,
                    FarragoMedMetadataQuery.OTN_TABLE);
            }
        }
        return null;
    }

    // implement FarragoMedNameDirectory
    public boolean queryMetadata(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink)
        throws SQLException
    {
        // NOTE:  We take advantage of the fact that we're permitted
        // to ignore query filters and return everything.

        if (scope.equals(FarragoMedMetadataQuery.OTN_SCHEMA)) {
            sink.writeObjectDescriptor(
                server.getForeignSchemaName(),
                FarragoMedMetadataQuery.OTN_SCHEMA,
                "Mock schema",
                new Properties());
        } else {
            sink.writeObjectDescriptor(
                server.getForeignTableName(),
                FarragoMedMetadataQuery.OTN_TABLE,
                "Mock table",
                new Properties());
            if (server.extractColumns) {
                sink.writeColumnDescriptor(
                    server.getForeignTableName(),
                    COLUMN_NAME,
                    0,
                    server.createMockColumnType(sink.getTypeFactory()),
                    "Mock column",
                    "0",
                    new Properties());
            }
        }
        return true;
    }
}

// End MedMockNameDirectory.java
