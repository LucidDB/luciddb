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
package net.sf.farrago.fennel.rel;

import java.util.*;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * FennelRenameRel is the Fennel implementation of a rename-only relational
 * Project operator (which is a no-op). It can work with any Fennel calling
 * convention.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelRenameRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    private String [] fieldNames;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelRenameRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child input rel whose fields are to be renamed
     * @param fieldNames new field names
     * @param traits traits for this rel
     */
    public FennelRenameRel(
        RelOptCluster cluster,
        RelNode child,
        String [] fieldNames,
        RelTraitSet traits)
    {
        super(cluster, traits, child);
        this.fieldNames = fieldNames;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelNode
    public boolean isDistinct()
    {
        return getChild().isDistinct();
    }

    // implement Cloneable
    public FennelRenameRel clone()
    {
        return new FennelRenameRel(
            getCluster(),
            getChild().clone(),
            fieldNames,
            cloneTraits());
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeTinyCost();
    }

    // implement RelNode
    public RelDataType deriveRowType()
    {
        final RelDataTypeField [] fields = getChild().getRowType().getFields();
        return getCluster().getTypeFactory().createStructType(
            new RelDataTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return fields.length;
                }

                public String getFieldName(int index)
                {
                    return fieldNames[index];
                }

                public RelDataType getFieldType(int index)
                {
                    return fields[index].getType();
                }
            });
    }

    // override Rel
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "child", "fieldNames" },
            new Object[] { Arrays.asList(fieldNames) });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        // no implementation needed for the rename itself, since that is done
        // implicitly by the returned row type
        return implementor.visitFennelChild((FennelRel) getChild(), 0);
    }
}

// End FennelRenameRel.java
