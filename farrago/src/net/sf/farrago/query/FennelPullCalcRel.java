/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

import net.sf.saffron.calc.RexToCalcTranslator;
import net.sf.saffron.core.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rex.*;

/**
 * FennelPullCalcRel is a pull-mode implementation of {@link FennelCalcRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelPullCalcRel
    extends FennelCalcRel
    implements FennelPullRel
{
    /**
     * Creates a new FennelPullCalcRel object.
     *
     * @param cluster VolcanoCluster for this rel
     * @param child rel producing rows to be Calced
     * @param rowType Row type
     * @param projectExprs Expressions returned by the calculator
     * @param conditionExpr Filter condition, may be null
     */
    public FennelPullCalcRel(
        VolcanoCluster cluster,
        SaffronRel child,
        SaffronType rowType,
        RexNode[] projectExprs,
        RexNode conditionExpr)
    {
        super(cluster,child,rowType,projectExprs,conditionExpr);
    }
    
    // implement SaffronRel
    public Object clone()
    {
        return new FennelPullCalcRel(
            cluster, OptUtil.clone(child), rowType,
            RexUtil.clone(getProjectExprs()),
            RexUtil.clone(getConditionExpr()));
    }

    // implement SaffronRel
    public CallingConvention getConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }
    
    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemCalcTupleStreamDef calcStream =
            getCatalog().newFemCalcTupleStreamDef();

        calcStream.getInput().add(
            implementor.visitFennelChild((FennelRel) child));
        calcStream.setFilter(getConditionExpr() != null);
        final RexToCalcTranslator translator = new RexToCalcTranslator(
                cluster.rexBuilder, getProjectExprs(), getConditionExpr());
        final String program = translator.getProgram(child.getRowType());
        calcStream.setProgram(program);
        return calcStream;
    }
}

// End FennelPullCalcRel.java
