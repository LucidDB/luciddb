/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package net.sf.saffron.oj.rel;

import openjava.ptree.ParseTree;
import openjava.ptree.StatementList;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;


/**
 * <code>JavaExistsRel</code> implements DISTINCT inline for the
 * special case that the input relation has zero columns.
 */
public class JavaExistsRel extends SingleRel implements JavaLoopRel
{
    public JavaExistsRel(
        RelOptCluster cluster,
        RelNode child)
    {
        super(cluster, new RelTraitSet(CallingConvention.JAVA), child);
        assert child.getRowType().getFieldList().size() == 0;
    }

    // implement RelNode
    public boolean isDistinct()
    {
        return true;
    }

    // implement RelNode
    public JavaExistsRel clone()
    {
        JavaExistsRel clone = new JavaExistsRel(getCluster(), getChild());
        clone.inheritTraitsFrom(this);

        return clone;
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // cheaper than JavaDistinct
        double dRows = 1;

        // cheaper than JavaDistinct
        double dCpu = dRows;

        // cheaper than JavaDistinct
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        // Generate
        //     <<parent>>
        return implementor.visitJavaChild(this, 0, (JavaRel) getChild());
    }

    public void implementJavaParent(
        JavaRelImplementor implementor,
        int ordinal)
    {
        assert ordinal == 0;

        // Generate
        //   <<parent-handler>>
        //   break;
        StatementList stmtList = implementor.getStatementList();
        implementor.generateParentBody(this, stmtList);
        stmtList.add(implementor.getExitStatement());
    }
}


// End JavaExistsRel.java
