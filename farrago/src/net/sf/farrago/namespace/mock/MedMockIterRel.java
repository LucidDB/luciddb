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

package net.sf.farrago.namespace.mock;

import net.sf.farrago.util.*;

import net.sf.saffron.opt.*;
import net.sf.saffron.core.*;
import net.sf.saffron.sql.*;
import net.sf.saffron.util.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.jdbc.*;
import net.sf.saffron.oj.stmt.*;
import net.sf.saffron.oj.rel.*;
import net.sf.saffron.oj.util.*;

import openjava.ptree.*;
import openjava.mop.*;

import java.sql.*;

/**
 * MedMockIterRel provides a mock implementation for
 * {@link TableAccessRel} with {@link CallingConvention.ITERATOR}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockIterRel extends TableAccessRel implements JavaRel
{
    private MedMockColumnSet columnSet;
    
    MedMockIterRel(
        MedMockColumnSet columnSet,
        VolcanoCluster cluster,
        SaffronConnection connection)
    {
        super(cluster,columnSet,connection);
        this.columnSet = columnSet;
    }

    public CallingConvention getConvention()
    {
        return CallingConvention.ITERATOR;
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        final SaffronType outputRowType = getRowType();
        OJClass outputRowClass = OJUtil.typeToOJClass(outputRowType);

        Expression newRowExp = new AllocationExpression(
            TypeName.forOJClass(outputRowClass),
            new ExpressionList());

        Expression iterExp = new AllocationExpression(
            TypeName.forClass(MedMockIterator.class),
            new ExpressionList(
                newRowExp,
                Literal.makeLiteral(columnSet.nRows)));
        
        return iterExp;
    }
}

// End MedMockIterRel.java
