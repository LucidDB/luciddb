/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.query;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.util.*;

import net.sf.saffron.core.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.util.*;

import java.util.*;


/**
 * FennelRenameRel is the Fennel implementation of a rename-only
 * relational Project operator (which is a no-op).  It can work with
 * any Fennel calling convention.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelRenameRel extends FennelSingleRel
{
    //~ Instance fields -------------------------------------------------------

    private String [] fieldNames;

    private CallingConvention convention;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelRenameRel object.
     *
     * @param cluster VolcanoCluster for this rel
     * @param child input rel whose fields are to be renamed
     * @param fieldNames new field names
     */
    public FennelRenameRel(
        VolcanoCluster cluster,
        SaffronRel child,
        String [] fieldNames,
        CallingConvention convention)
    {
        super(cluster,child);
        this.fieldNames = fieldNames;
        this.convention = convention;
    }

    //~ Methods ---------------------------------------------------------------

    // implement SaffronRel
    public CallingConvention getConvention()
    {
        return convention;
    }
    
    // implement SaffronRel
    public boolean isDistinct()
    {
        return child.isDistinct();
    }

    // implement Cloneable
    public Object clone()
    {
        return new FennelRenameRel(
            cluster,OptUtil.clone(child),fieldNames,convention);
    }

    // implement SaffronRel
    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        return planner.makeTinyCost();
    }

    // implement SaffronRel
    public SaffronType deriveRowType()
    {
        final SaffronField [] fields = child.getRowType().getFields();
        return cluster.typeFactory.createProjectType(
            new SaffronTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return fields.length;
                }

                public String getFieldName(int index)
                {
                    return fieldNames[index];
                }

                public SaffronType getFieldType(int index)
                {
                    return fields[index].getType();
                }
            });
    }

    // override Rel
    public void explain(PlanWriter pw)
    {
        pw.explain(
            this,
            new String [] { "child","fieldNames" },
            new Object [] { Arrays.asList(fieldNames) });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FarragoRelImplementor implementor)
    {
        // no implementation needed for the rename itself, since that is done
        // implicitly by the returned row type
        return implementor.implementFennelRel(child);
    }
}


// End FennelRenameRel.java
