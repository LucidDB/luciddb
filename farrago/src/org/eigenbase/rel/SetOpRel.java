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
package org.eigenbase.rel;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * <code>SetOpRel</code> is an abstract base for relational set operators such
 * as union, minus, and intersect.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class SetOpRel
    extends AbstractRelNode
{
    //~ Instance fields --------------------------------------------------------

    protected RelNode [] inputs;
    protected boolean all;

    //~ Constructors -----------------------------------------------------------

    protected SetOpRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode [] inputs,
        boolean all)
    {
        super(cluster, traits);
        this.inputs = inputs;
        this.all = all;
    }

    //~ Methods ----------------------------------------------------------------

    public abstract SetOpRel clone(RelNode [] inputs, boolean all);

    public boolean isDistinct()
    {
        return !all;
    }

    public RelNode [] getInputs()
    {
        return inputs;
    }

    public void explain(RelOptPlanWriter pw)
    {
        String [] terms = new String[inputs.length + 1];
        for (int i = 0; i < inputs.length; i++) {
            terms[i] = "input#" + i;
        }
        terms[inputs.length] = "all";
        pw.explain(
            this,
            terms,
            new Object[] { Boolean.valueOf(all) });
    }

    public void replaceInput(
        int ordinalInParent,
        RelNode p)
    {
        inputs[ordinalInParent] = p;
    }

    protected RelDataType deriveRowType()
    {
        RelDataType [] types = new RelDataType[inputs.length];
        for (int i = 0; i < inputs.length; i++) {
            types[i] = inputs[i].getRowType();
        }
        return getCluster().getTypeFactory().leastRestrictive(types);
    }

    /**
     * Returns whether all the inputs of this set operator have the same row
     * type as its output row.
     *
     * @param compareNames whether or not column names are important in the
     * homogeneity comparison
     */
    public boolean isHomogeneous(boolean compareNames)
    {
        RelDataType unionType = getRowType();
        RelNode [] inputs = getInputs();
        for (int i = 0; i < inputs.length; ++i) {
            RelDataType inputType = inputs[i].getRowType();
            if (!RelOptUtil.areRowTypesEqual(
                    inputType,
                    unionType,
                    compareNames))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether all the inputs of this set operator have the same row
     * type as its output row. Equivalent to {@link #isHomogeneous(boolean)
     * isHomogeneous(true)}.
     */
    public boolean isHomogeneous()
    {
        return isHomogeneous(true);
    }
}

// End SetOpRel.java
