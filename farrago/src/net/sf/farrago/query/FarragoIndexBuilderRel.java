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
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.med.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql2rel.*;


/**
 * FarragoIndexBuilderRel is the abstract relational expression corresponding to
 * building an index on a table. It is declared here rather than in {@link
 * org.eigenbase.rel} because indexes are not part of pure relational algebra.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoIndexBuilderRel
    extends SingleRel
    implements RelStructuredTypeFlattener.SelfFlatteningRel
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Index to be built.
     */
    private final FemLocalIndex index;

    /**
     * Table index belongs to
     */
    private final RelOptTable table;

    //~ Constructors -----------------------------------------------------------

    public FarragoIndexBuilderRel(
        RelOptCluster cluster,
        RelOptTable table,
        RelNode child,
        FemLocalIndex index)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            child);
        this.table = table;
        this.index = index;
    }

    //~ Methods ----------------------------------------------------------------

    public FemLocalIndex getIndex()
    {
        return index;
    }

    public RelOptTable getTable()
    {
        return table;
    }

    // implement Cloneable
    public FarragoIndexBuilderRel clone()
    {
        FarragoIndexBuilderRel clone =
            new FarragoIndexBuilderRel(
                getCluster(),
                getTable(),
                getChild().clone(),
                index);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelDataType deriveRowType()
    {
        return RelOptUtil.createDmlRowType(
            getCluster().getTypeFactory());
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "child", "index" },
            new Object[] {
                Arrays.asList(
                    FarragoCatalogUtil.getQualifiedName(index).names)
            });
    }

    // implement RelStructuredTypeFlattener.SelfFlatteningRel
    public void flattenRel(RelStructuredTypeFlattener flattener)
    {
        flattener.rewriteGeneric(this);
    }
}

// End FarragoIndexBuilderRel.java
