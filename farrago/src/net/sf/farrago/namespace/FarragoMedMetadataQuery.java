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

import java.util.*;


/**
 * FarragoMedMetadataQuery defines a metadata query processed by {@link
 * FarragoMedNameDirectory}. For some namespaces, it may be more efficient to
 * query different kinds of metadata simultaneously; for example, a flat-file
 * reader might return all files in a directory as tables, with the header of
 * each file providing a column list. For this reason, the interface allows more
 * than one object type to be queried together.
 *
 * <p>Object types are identified using CWM class names. The following types are
 * currently defined (see members starting with prefix "OTN_"):
 *
 * <ul>
 * <li><em>Table</em>: any table-like object such as a base table or view
 * <li><em>Column</em>: a column
 * <li><em>Schema</em>: a schema (acts as a subdirectory)
 * <li><em>Package</em>: a logical package (acts as a subdirectory)
 * </ul>
 *
 * <p>Results from metadata queries are written via the {@link
 * FarragoMedMetadataSink} interface. Only immediate contents of the queried
 * directory should be returned; some results may be subdirectories which can be
 * queried independently.
 *
 * <p>Note that implementation of query filtering and projection is always
 * optional. An unsophisticated implementation is free to produce results which
 * do not match all restrictions, so consumers are responsible for reapplying
 * them to each object produced.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoMedMetadataQuery
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String OTN_TABLE = "Table";

    public static final String OTN_COLUMN = "Column";

    public static final String OTN_SCHEMA = "Schema";

    public static final String OTN_PACKAGE = "Package";

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves a map of filters to be applied to queried metadata. Map keys
     * are object type names, and values are instances of the {@link
     * FarragoMedMetadataFilter} interface. The conjunction of all relevant
     * filters is used. Metadata queries take place in the context of an
     * instance of {@link FarragoMedNameDirectory}, which provides additional
     * filtering context.
     *
     * @return map
     */
    public Map<String, FarragoMedMetadataFilter> getFilterMap();

    /**
     * Retrieves a set of result object types to be returned by the query,
     * identified by object type name.
     *
     * @return Set<String>
     */
    public Set<String> getResultObjectTypes();
}

// End FarragoMedMetadataQuery.java
