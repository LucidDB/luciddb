/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2003 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
