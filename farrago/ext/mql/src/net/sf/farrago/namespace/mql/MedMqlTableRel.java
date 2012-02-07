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
package net.sf.farrago.namespace.mql;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

/**
 * MedMqlTableRel represents a logical access to an MQL foreign table
 * before any pushdown.
 *
 * @author John Sichi
 * @version $Id$
 */
class MedMqlTableRel extends TableAccessRelBase
{
    public MedMqlTableRel(
        RelOptCluster cluster,
        RelOptTable table,
        RelOptConnection connection)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            table,
            connection);
    }

    MedMqlColumnSet getMedMqlColumnSet()
    {
        return (MedMqlColumnSet) getTable();
    }
}

// End MedMqlTableRel.java
