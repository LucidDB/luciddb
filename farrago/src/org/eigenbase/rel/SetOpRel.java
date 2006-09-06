/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
     *                     homogeneity comparison
     */
    public boolean isHomogeneous(boolean compareNames)
    {
        RelDataType unionType = getRowType();
        RelNode [] inputs = getInputs();
        for (int i = 0; i < inputs.length; ++i) {
            RelDataType inputType = inputs[i].getRowType();
            if (!RelOptUtil.areRowTypesEqual(
                    inputType, unionType, compareNames)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Returns whether all the inputs of this set operator have the same row
     * type as its output row.  Equivalent to 
     * {@link #isHomogeneous(boolean) isHomogeneous(true)}.
     */
    public boolean isHomogeneous()
    {
        return isHomogeneous(true);
    }
}

// End SetOpRel.java
