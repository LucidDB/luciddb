/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package net.sf.farrago.namespace.mock;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql2rel.*;


/**
 * MedMockIterRel provides a mock implementation for {@link TableAccessRel} with
 * {@link CallingConvention.ITERATOR}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockIterRel
    extends TableAccessRelBase
    implements JavaRel,
        RelStructuredTypeFlattener.SelfFlatteningRel
{

    //~ Instance fields --------------------------------------------------------

    private MedMockColumnSet columnSet;

    //~ Constructors -----------------------------------------------------------

    MedMockIterRel(
        MedMockColumnSet columnSet,
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.ITERATOR),
            columnSet,
            connection);
        this.columnSet = columnSet;
    }

    //~ Methods ----------------------------------------------------------------

    public ParseTree implement(JavaRelImplementor implementor)
    {
        final RelDataType outputRowType = getRowType();
        OJClass outputRowClass =
            OJUtil.typeToOJClass(
                outputRowType,
                implementor.getTypeFactory());

        Expression newRowExp =
            new AllocationExpression(
                TypeName.forOJClass(outputRowClass),
                new ExpressionList());

        Expression iterExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(MedMockTupleIter.class),
                new ExpressionList(
                    newRowExp,
                    Literal.makeLiteral(columnSet.nRows)));

        return iterExp;
    }

    // implement RelNode
    public MedMockIterRel clone()
    {
        MedMockIterRel clone =
            new MedMockIterRel(
                columnSet,
                getCluster(),
                connection);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelStructuredTypeFlattener.SelfFlatteningRel
    public void flattenRel(RelStructuredTypeFlattener flattener)
    {
        flattener.rewriteGeneric(this);
    }
}

// End MedMockIterRel.java
