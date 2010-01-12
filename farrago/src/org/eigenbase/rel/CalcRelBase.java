/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2010 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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

import java.util.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * <code>CalcRelBase</code> is an abstract base class for implementations of
 * {@link CalcRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class CalcRelBase
    extends SingleRel
{
    //~ Instance fields --------------------------------------------------------

    protected final RexProgram program;
    private final List<RelCollation> collationList;

    //~ Constructors -----------------------------------------------------------

    protected CalcRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelDataType rowType,
        RexProgram program,
        List<RelCollation> collationList)
    {
        super(cluster, traits, child);
        this.rowType = rowType;
        this.program = program;
        this.collationList =
            collationList.isEmpty() ? Collections.<RelCollation>emptyList()
            : collationList;
        assert isValid(true);
    }

    //~ Methods ----------------------------------------------------------------

    public boolean isValid(boolean fail)
    {
        if (!RelOptUtil.equal(
                "program's input type",
                program.getInputRowType(),
                "child's output type",
                getChild().getRowType(),
                fail))
        {
            return false;
        }
        if (!RelOptUtil.equal(
                "rowtype of program",
                program.getOutputRowType(),
                "declared rowtype of rel",
                rowType,
                fail))
        {
            return false;
        }
        if (!program.isValid(fail)) {
            return false;
        }
        if (!program.isNormalized(fail, getCluster().getRexBuilder())) {
            return false;
        }
        if (!RelCollationImpl.isValid(
                getRowType(),
                collationList,
                fail))
        {
            return false;
        }
        return true;
    }

    public RexProgram getProgram()
    {
        return program;
    }

    public double getRows()
    {
        return FilterRel.estimateFilteredRows(
            getChild(),
            program);
    }

    public List<RelCollation> getCollationList()
    {
        return collationList;
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = RelMetadataQuery.getRowCount(this);
        double dCpu =
            RelMetadataQuery.getRowCount(getChild())
            * program.getExprCount();
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    public RexNode [] getChildExps()
    {
        return RexNode.EMPTY_ARRAY;
    }

    public void explain(RelOptPlanWriter pw)
    {
        program.explainCalc(this, pw);
    }
}

// End CalcRelBase.java
