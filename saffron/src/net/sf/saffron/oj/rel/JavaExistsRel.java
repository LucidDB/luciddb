/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.oj.rel;

import org.eigenbase.oj.rel.*;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.rel.DistinctRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.util.Util;
import openjava.ptree.ParseTree;
import openjava.ptree.StatementList;


/**
 * <code>JavaExistsRel</code> implements {@link DistinctRel} inline for the
 * special case that the input relation has zero columns.
 */
public class JavaExistsRel extends DistinctRel implements JavaLoopRel
{
    //~ Constructors ----------------------------------------------------------

    public JavaExistsRel(RelOptCluster cluster,RelNode child)
    {
        super(cluster,child);
        assert child.getRowType().getFieldCount() == 0;
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.JAVA;
    }

    // implement RelNode
    public Object clone()
    {
        return new JavaExistsRel(cluster,child);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // cheaper than JavaDistinct
        double dRows = 1;

        // cheaper than JavaDistinct
        double dCpu = dRows;

        // cheaper than JavaDistinct
        double dIo = 0;
        return planner.makeCost(dRows,dCpu,dIo);
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        // Generate
        //     <<parent>>
        return implementor.visitJavaChild(this, 0, (JavaRel) child);
    }

    public void implementJavaParent(JavaRelImplementor implementor, int ordinal) {
        assert ordinal == 0;
        // Generate
        //   <<parent-handler>>
        //   break;
        StatementList stmtList = implementor.getStatementList();
        implementor.generateParentBody(this,stmtList);
        stmtList.add(implementor.getExitStatement());
    }
}


// End JavaExistsRel.java
