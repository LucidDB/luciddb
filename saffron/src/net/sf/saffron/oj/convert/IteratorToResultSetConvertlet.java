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

package net.sf.saffron.oj.convert;

import openjava.mop.OJClass;
import openjava.ptree.*;

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.convert.ConverterRel;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.runtime.IteratorResultSet;
import org.eigenbase.util.Util;


/**
 * Thunk to convert between {@link CallingConvention#ITERATOR iterator}
 * and {@link CallingConvention#RESULT_SET result-set} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class IteratorToResultSetConvertlet extends JavaConvertlet
{
    public IteratorToResultSetConvertlet()
    {
        super(CallingConvention.ITERATOR, CallingConvention.RESULT_SET);
    }

    public ParseTree implement(
        JavaRelImplementor implementor,
        ConverterRel converter)
    {
        Object o =
            implementor.visitJavaChild(converter, 0, (JavaRel) converter.child);
        final RelDataType rowType = converter.getRowType();
        OJClass rowClass = OJUtil.typeToOJClass(
            rowType, implementor.getTypeFactory());
        Expression getter;
        if (true) {
            getter =
                new AllocationExpression(
                    TypeName.forOJClass(
                        OJClass.forClass(IteratorResultSet.FieldGetter.class)),
                    new ExpressionList(
                        new FieldAccess(
                            TypeName.forOJClass(rowClass),
                            "class")));
        } else if (rowType.isStruct() && (rowType.getFieldCount() == 1)) {
            getter =
                new AllocationExpression(
                    TypeName.forOJClass(
                        OJClass.forClass(
                            IteratorResultSet.SingletonColumnGetter.class)),
                    new ExpressionList());
        } else {
            getter =
                new AllocationExpression(
                    TypeName.forOJClass(
                        OJClass.forClass(
                            IteratorResultSet.SyntheticColumnGetter.class)),
                    new ExpressionList(
                        new FieldAccess(
                            TypeName.forOJClass(rowClass),
                            "class")));
        }
        return new AllocationExpression(
            TypeName.forOJClass(OJClass.forClass(IteratorResultSet.class)),
            new ExpressionList(
                new CastExpression(Util.clazzIterator, (Expression) o),
                getter));
    }
}


// End IteratorToResultSetConvertlet.java
