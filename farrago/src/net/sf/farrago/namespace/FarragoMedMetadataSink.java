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

import net.sf.farrago.type.*;

import org.eigenbase.reltype.*;


/**
 * FarragoMedMetadataSink provides a target for instances of {@link
 * FarragoMedNameDirectory} to write metadata results in response to a {@link
 * FarragoMedMetadataQuery}. Results must be written in dependency order (e.g. a
 * table before its columns), and columns must be written in ordinal order.
 *
 * <p>Results may be filtered as they are written, in which case the sink
 * reports the filter result back to the caller.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoMedMetadataSink
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Writes a generic descriptor for an object. Some objects (such as columns)
     * have more specific write methods.
     *
     * @param name unqualified object name
     * @param remarks object description, or null for none
     * @param properties storage options
     *
     * @return true if object was accepted; false if object was filtered out
     */
    public boolean writeObjectDescriptor(
        String name,
        String typeName,
        String remarks,
        Properties properties);

    /**
     * Writes a descriptor for a column.
     *
     * @param tableName unqualified table name
     * @param columnName unqualified column name
     * @param ordinal 0-based ordinal of column within table
     * @param type column datatype
     * @param remarks column description, or null for none
     * @param defaultValue column default value, or null for none
     * @param properties storage options
     *
     * @return true if object was accepted; false if object was filtered out
     */
    public boolean writeColumnDescriptor(
        String tableName,
        String columnName,
        int ordinal,
        RelDataType type,
        String remarks,
        String defaultValue,
        Properties properties);

    /**
     * @return a type factory for use in creating type instances for calls such
     * as {@link #writeColumnDescriptor}
     */
    public FarragoTypeFactory getTypeFactory();
}

// End FarragoMedMetadataSink.java
