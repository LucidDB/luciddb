/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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

package com.disruptivetech.farrago.rel;

import com.disruptivetech.farrago.calc.RexToCalcTranslator;
import com.disruptivetech.farrago.volcano.*;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * FennelPullCalcRel is a pull-mode implementation of {@link FennelCalcRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelPullCalcRel extends FennelCalcRel implements FennelPullRel
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelPullCalcRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child rel producing rows to be Calced
     * @param rowType Row type
     * @param projectExprs Expressions returned by the calculator
     * @param conditionExpr Filter condition, may be null
     */
    public FennelPullCalcRel(
        RelOptCluster cluster,
        RelNode child,
        RelDataType rowType,
        RexNode [] projectExprs,
        RexNode conditionExpr)
    {
        super(cluster, new RelTraitSet(FENNEL_PULL_CONVENTION), child, rowType,
            projectExprs, conditionExpr);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public Object clone()
    {
        RexNode clonedConditionExpr =
            getConditionExpr() != null
            ? RexUtil.clone(getConditionExpr())
            : null;

        FennelPullCalcRel clone = new FennelPullCalcRel(
            getCluster(),
            RelOptUtil.clone(getChild()),
            rowType,
            RexUtil.clone(getProjectExprs()),
            clonedConditionExpr);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemCalcTupleStreamDef calcStream =
            implementor.getMetadataFactory().newFemCalcTupleStreamDef();

        calcStream.getInput().add(
            implementor.visitFennelChild((FennelRel) getChild()));
        calcStream.setFilter(getConditionExpr() != null);
        final RexToCalcTranslator translator =
            new RexToCalcTranslator(getCluster().getRexBuilder());
        final String program = translator.getProgram(
            getChild().getRowType(),
            getProjectExprs(),
            getConditionExpr());
        calcStream.setProgram(program);
        return calcStream;
    }
}


// End FennelPullCalcRel.java
