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

package net.sf.saffron.oj.util;

import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.oj.OJTypeFactory;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.util.Util;
import openjava.mop.OJClass;
import openjava.ptree.Expression;
import openjava.ptree.Literal;
import openjava.ptree.TypeName;

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

    public static OJClass ojClassForExpression(SaffronRel rel,Expression exp)
    {
        try {
            return exp.getType(new RelEnvironment(rel));
        } catch (Exception e) {
            throw Util.newInternal(e);
        }
    }

    public static SaffronType ojToType(
        SaffronTypeFactory typeFactory,
        OJClass ojClass)
    {
        if (ojClass == null) {
            return null;
        }
        return ((OJTypeFactory) typeFactory).toType(ojClass);
    }

    /**
     * Converts a {@link SaffronType} to a {@link TypeName}.
     *
     * @pre threadDeclarers.get() != null
     */
    public static TypeName toTypeName(SaffronType rowType)
    {
        return TypeName.forOJClass(typeToOJClass(rowType));
    }

    public static OJClass typeToOJClass(OJClass declarer,SaffronType rowType)
    {
        OJTypeFactory typeFactory = (OJTypeFactory) rowType.getFactory();
        return typeFactory.toOJClass(declarer,rowType);
    }

    /**
     * Converts a {@link SaffronType} to a {@link OJClass}.
     *
     * @pre threadDeclarers.get() != null
     */
    public static OJClass typeToOJClass(SaffronType rowType)
    {
        OJClass declarer = (OJClass) threadDeclarers.get();
        if (declarer == null) {
            assert (false) : "threadDeclarers.get() != null";
        }
        return typeToOJClass(declarer,rowType);
    }

    public static Object literalValue(Literal literal) {
        String value = literal.toString();
        switch (literal.getLiteralType()) {
        case Literal.BOOLEAN:
            return value.equals("true") ? Boolean.TRUE : Boolean.FALSE;
        case Literal.INTEGER:
            return new Integer(Integer.parseInt(value));
        case Literal.LONG:
            value = value.substring(0,value.length() - 1); // remove 'l'
            return new Integer(Integer.parseInt(value));
        case Literal.FLOAT:
            value = value.substring(0,value.length() - 1); // remove 'f'
            return new Double(Double.parseDouble(value));
        case Literal.DOUBLE:
            value = value.substring(0,value.length() - 1); // remove 'd'
            return new Double(Double.parseDouble(value));
        case Literal.CHARACTER:
            return value.substring(1,2); // 'x' --> x
        case Literal.STRING:
            return Util.stripDoubleQuotes(value); // "foo" --> foo
        case Literal.NULL:
            return null;
        default:
            throw Util.newInternal(
                "unknown literal type " + literal.getLiteralType());
        }
    }
}


// End OJUtil.java
