/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
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

package org.eigenbase.oj.util;

import openjava.mop.OJClass;
import openjava.ptree.Expression;
import openjava.ptree.Literal;
import openjava.ptree.TypeName;

import org.eigenbase.oj.OJTypeFactory;
import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.util.Util;


/**
 * Static utilities for manipulating OpenJava expressions.
 */
public abstract class OJUtil
{
    //~ Static fields/initializers --------------------------------------------

    /**
     * Each thread's enclosing {@link OJClass}. Synthetic classes are declared
     * as inner classes of this.
     */
    public static final ThreadLocal threadDeclarers = new ThreadLocal();

    //~ Methods ---------------------------------------------------------------

    public static OJClass ojClassForExpression(
        RelNode rel,
        Expression exp)
    {
        try {
            return exp.getType(new RelEnvironment(rel));
        } catch (Exception e) {
            throw Util.newInternal(e);
        }
    }

    public static RelDataType ojToType(
        RelDataTypeFactory typeFactory,
        OJClass ojClass)
    {
        if (ojClass == null) {
            return null;
        }
        return ((OJTypeFactory) typeFactory).toType(ojClass);
    }

    /**
     * Converts a {@link RelDataType} to a {@link TypeName}.
     *
     * @pre threadDeclarers.get() != null
     */
    public static TypeName toTypeName(RelDataType rowType)
    {
        return TypeName.forOJClass(typeToOJClass(rowType));
    }

    public static OJClass typeToOJClass(
        OJClass declarer,
        RelDataType rowType)
    {
        OJTypeFactory typeFactory = (OJTypeFactory) rowType.getFactory();
        return typeFactory.toOJClass(declarer, rowType);
    }

    /**
     * Converts a {@link RelDataType} to a {@link OJClass}.
     *
     * @pre threadDeclarers.get() != null
     */
    public static OJClass typeToOJClass(RelDataType rowType)
    {
        OJClass declarer = (OJClass) threadDeclarers.get();
        if (declarer == null) {
            assert (false) : "threadDeclarers.get() != null";
        }
        return typeToOJClass(declarer, rowType);
    }

    public static Object literalValue(Literal literal)
    {
        String value = literal.toString();
        switch (literal.getLiteralType()) {
        case Literal.BOOLEAN:
            return value.equals("true") ? Boolean.TRUE : Boolean.FALSE;
        case Literal.INTEGER:
            return new Integer(Integer.parseInt(value));
        case Literal.LONG:
            value = value.substring(0, value.length() - 1); // remove 'l'
            return new Integer(Integer.parseInt(value));
        case Literal.FLOAT:
            value = value.substring(0, value.length() - 1); // remove 'f'
            return new Double(Double.parseDouble(value));
        case Literal.DOUBLE:
            value = value.substring(0, value.length() - 1); // remove 'd'
            return new Double(Double.parseDouble(value));
        case Literal.CHARACTER:
            return value.substring(1, 2); // 'x' --> x
        case Literal.STRING:
            return Util.stripDoubleQuotes(value); // "foo" --> foo
        case Literal.NULL:
            return null;
        default:
            throw Util.newInternal("unknown literal type "
                + literal.getLiteralType());
        }
    }

    public static TypeName typeNameForClass(Class clazz)
    {
        return TypeName.forOJClass(OJClass.forClass(clazz));
    }
}


// End OJUtil.java
