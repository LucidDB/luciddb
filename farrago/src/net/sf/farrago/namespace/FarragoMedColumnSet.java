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

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.validate.*;


/**
 * FarragoMedColumnSet defines an interface for all relation-like objects
 * accessible by Farrago. Instances of FarragoMedColumnSet are not necessarily
 * described in Farrago's catalog. However, when they are, they are described by
 * instances of CwmNamedColumnSet.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoMedColumnSet
    extends RelOptTable,
        SqlValidatorTable
{
    /**
     * Returns the fully-qualified name by which this ColumnSet is known within
     * the Farrago system.
     *
     * @return the fully-qualified name by which this ColumnSet is known by
     *     within the Farrago system
     */
    public String [] getLocalName();

    /**
     * Returns the fully-qualified name by which this ColumnSet is known on the
     * foreign server.
     *
     * @return the fully-qualified name by which this ColumnSet is known on the
     *     foreign server
     */
    public String [] getForeignName();

    /**
     * Returns the name of this ColumnSet.
     *
     * @return the name of this ColumnSet
     */
    public String getName();

    /**
     * Sets the row type of this ColumnSet.
     *
     * @param rowType Row type
     */
    void setRowType(RelDataType rowType);
}

// End FarragoMedColumnSet.java
