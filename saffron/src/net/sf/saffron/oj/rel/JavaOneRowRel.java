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

import net.sf.saffron.oj.util.*;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.RelImplementor;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.OneRowRel;
import net.sf.saffron.util.Util;

import openjava.ptree.AllocationExpression;
import openjava.ptree.Expression;
import openjava.ptree.ExpressionList;
import openjava.ptree.StatementList;


/**
 * <code>JavaOneRowRel</code> implements {@link OneRowRel} inline.
 */
public class JavaOneRowRel extends OneRowRel implements JavaRel
{
    //~ Constructors ----------------------------------------------------------

    public JavaOneRowRel(VolcanoCluster cluster)
    {
        super(cluster);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.JAVA;
    }

    // implement SaffronRel
    public Object clone()
    {
        return new JavaOneRowRel(cluster);
    }

    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1:

            // Generate
            //	   <<parent>>
            StatementList stmtList = implementor.getStatementList();
            implementor.generateParentBody(this,stmtList);
            return null;
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    public Expression implementSelf(RelImplementor implementor)
    {
        return new AllocationExpression(
            OJUtil.typeToOJClass(getRowType()),
            new ExpressionList());
    }
}


// End JavaOneRowRel.java
