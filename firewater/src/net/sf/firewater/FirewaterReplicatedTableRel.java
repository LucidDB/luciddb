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
package net.sf.firewater;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

/**
 * FirewaterReplicatedTableRel represents a replicated table in
 * a query plan before we have chosen which replica to access
 * (after which it is typically replaced by a JDBC query).
 *
 * @author John Sichi
 * @version $Id$
 */
public class FirewaterReplicatedTableRel extends TableAccessRelBase
{
    /**
     * Refinement for super.table.
     */
    final FirewaterColumnSet replicatedTable;

    /**
     * Creates a new FirewaterReplicatedTableRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param replicatedTable table being accessed
     * @param connection connection
     */
    public FirewaterReplicatedTableRel(
        RelOptCluster cluster,
        FirewaterColumnSet replicatedTable,
        RelOptConnection connection)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            replicatedTable,
            connection);
        this.replicatedTable = replicatedTable;
    }
}

// End FirewaterReplicatedTableRel.java
