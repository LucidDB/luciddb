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
import org.eigenbase.oj.util.*;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.rel.OneRowRel;
import org.eigenbase.util.Util;

import openjava.ptree.*;


/**
 * <code>JavaOneRowRel</code> implements {@link OneRowRel} inline.
 */
public class JavaOneRowRel extends OneRowRel
        implements JavaLoopRel, JavaSelfRel
{
    //~ Constructors ----------------------------------------------------------

    public JavaOneRowRel(RelOptCluster cluster)
    {
        super(cluster);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.JAVA;
    }

    // implement RelNode
    public Object clone()
    {
        return new JavaOneRowRel(cluster);
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        // Generate
        //	   <<parent>>
        StatementList stmtList = implementor.getStatementList();
        implementor.generateParentBody(this,stmtList);
        return null;
    }

    public void implementJavaParent(JavaRelImplementor implementor,
            int ordinal) {
        throw Util.newInternal("should never be called");
    }

    public Expression implementSelf(JavaRelImplementor implementor)
    {
        return new AllocationExpression(
            OJUtil.typeToOJClass(getRowType()),
            new ExpressionList());
    }
}


// End JavaOneRowRel.java
