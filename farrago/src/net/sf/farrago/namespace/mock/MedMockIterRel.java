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

import java.sql.*;

import net.sf.farrago.util.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.stmt.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.jdbc.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * MedMockIterRel provides a mock implementation for
 * {@link TableAccessRel} with {@link CallingConvention.ITERATOR}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockIterRel extends TableAccessRel implements JavaRel
{
    //~ Instance fields -------------------------------------------------------

    private MedMockColumnSet columnSet;

    //~ Constructors ----------------------------------------------------------

    MedMockIterRel(
        MedMockColumnSet columnSet,
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        super(cluster, columnSet, connection);
        this.columnSet = columnSet;
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.ITERATOR;
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        final RelDataType outputRowType = getRowType();
        OJClass outputRowClass = OJUtil.typeToOJClass(
            outputRowType,
            implementor.getTypeFactory());

        Expression newRowExp =
            new AllocationExpression(
                TypeName.forOJClass(outputRowClass),
                new ExpressionList());

        Expression iterExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(MedMockIterator.class),
                new ExpressionList(
                    newRowExp,
                    Literal.makeLiteral(columnSet.nRows)));

        return iterExp;
    }
}


// End MedMockIterRel.java
