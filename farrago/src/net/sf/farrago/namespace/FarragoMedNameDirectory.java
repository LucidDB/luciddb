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
package net.sf.farrago.namespace;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.type.*;


/**
 * FarragoMedNameDirectory defines a virtual hierarchical namespace interface in
 * which to look up tables, routines, other namespaces, etc.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoMedNameDirectory
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Looks up a FarragoMedColumnSet by name. This method supports Farrago's
     * capability to reference a foreign table directly without having to create
     * local metadata about it.
     *
     * @param typeFactory FarragoTypeFactory to use for defining types
     * @param foreignName simple name of foreign ColumnSet to lookup as a direct
     * child of this directory
     * @param localName compound identifier by which FarragoMedColumnSet will be
     * referenced locally
     *
     * @return FarragoMedColumnSet, or null if none found
     *
     * @exception SQLException if metadata access is unsuccessful
     */
    public FarragoMedColumnSet lookupColumnSet(
        FarragoTypeFactory typeFactory,
        String foreignName,
        String [] localName)
        throws SQLException;

    /**
     * Looks up an immediate subdirectory by name.
     *
     * @param foreignName identifier for subdirectory
     *
     * @return subdirectory, or null if none found
     *
     * @exception SQLException if metadata access is unsuccessful
     */
    public FarragoMedNameDirectory lookupSubdirectory(String foreignName)
        throws SQLException;

    /**
     * Executes a query against the metadata contained by this directory. This
     * method supports the SQL/MED IMPORT FOREIGN SCHEMA statement, and general
     * metadata browsing.
     *
     * <p>NOTE: the supplied sink may be used to implement passive aborts by
     * throwing an unchecked exception when an abort request is detected.
     *
     * @param query the query to execute
     * @param sink target which receives the query results
     *
     * @return true if the query executed successfully; false if the requested
     * query type was not supported
     *
     * @exception SQLException if metadata access is unsuccessful (but not if
     * query is unsupported)
     */
    public boolean queryMetadata(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink)
        throws SQLException;

    /**
     * Creates a new instance of FemBaseColumnSet in the catalog to represent an
     * imported table.
     *
     * @param repos repository storing catalog
     * @param tableName name of imported table
     *
     * @return new object in catalog
     */
    public FemBaseColumnSet newImportedColumnSet(
        FarragoRepos repos,
        String tableName);
}

// End FarragoMedNameDirectory.java
