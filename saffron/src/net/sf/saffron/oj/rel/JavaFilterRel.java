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

import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.OptUtil;
import net.sf.saffron.opt.RelImplementor;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.FilterRel;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexUtil;
import net.sf.saffron.util.Util;
import openjava.ptree.Expression;
import openjava.ptree.IfStatement;
import openjava.ptree.StatementList;


/**
 * Implements the {@link FilterRel} relational expression in Java code.
 */
public class JavaFilterRel extends FilterRel
{
    //~ Constructors ----------------------------------------------------------

    public JavaFilterRel(
        VolcanoCluster cluster,
        SaffronRel child,
        RexNode condition)
    {
        super(cluster,child,condition);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.JAVA;
    }

    public Object clone()
    {
        return new JavaFilterRel(
            cluster,
            OptUtil.clone(child),
            RexUtil.clone(condition));
    }

    // implement SaffronRel
    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
            return implementor.implementChild(this,0,child);
        case 0: // called from child
            StatementList stmtList = implementor.getStatementList();
            StatementList ifBody = new StatementList();
            Expression condition2 = implementor.translate(this,condition);
            stmtList.add(new IfStatement(condition2,ifBody));
            implementor.bind(this,child);
            implementor.generateParentBody(this,ifBody);
            return null;
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }
}


// End JavaFilterRel.java
