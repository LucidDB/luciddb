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

import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.oj.util.OJUtil;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.PlanCost;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.DistinctRel;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.util.Util;
import openjava.mop.Toolbox;
import openjava.ptree.*;


/**
 * <code>JavaDistinctRel</code> implements {@link DistinctRel} inline. See
 * also {@link JavaAggregateRel}.
 */
public class JavaDistinctRel extends DistinctRel implements JavaLoopRel
{
    //~ Instance fields -------------------------------------------------------

    Variable var_h;

    //~ Constructors ----------------------------------------------------------

    public JavaDistinctRel(VolcanoCluster cluster,SaffronRel child)
    {
        super(cluster,child);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.JAVA;
    }

    // implement SaffronRel
    public Object clone()
    {
        return new JavaDistinctRel(cluster,child);
    }

    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        double dRows = child.getRows();
        double dCpu = Util.nLogN(dRows);
        double dIo = 0;
        return planner.makeCost(dRows,dCpu,dIo);
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        // Generate
        //	   HashSet h = new HashSet();
        //	   <<parent>>
        StatementList stmtList = implementor.getStatementList();
        this.var_h = implementor.newVariable();
        stmtList.add(
                new VariableDeclaration(
                        new TypeName("java.util.HashSet"),
                        var_h.toString(),
                        new AllocationExpression(
                                new TypeName("java.util.HashSet"),
                                null)));
        return implementor.visitJavaChild(this, 0, (JavaRel) child);
    }

    public void implementJavaParent(JavaRelImplementor implementor,
            int ordinal) {
        assert ordinal == 0;
        // Generate
        //	 if (h.add(<<child variable>>)) {
        //	   <<parent-handler>>
        //	 }
        StatementList stmtList = implementor.getStatementList();
        StatementList ifBody = new StatementList();
        stmtList.add(
            new IfStatement(
                new MethodCall(
                    this.var_h,
                    "add",
                    new ExpressionList(
                        Toolbox.box(
                            OJUtil.typeToOJClass(child.getRowType()),
                            implementor.translateInput(this,0)))),
                ifBody));
        implementor.bind(this,child);
        implementor.generateParentBody(this,ifBody);
    }
}


// End JavaDistinctRel.java
