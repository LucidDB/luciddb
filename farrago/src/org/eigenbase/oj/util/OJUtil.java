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

import openjava.mop.*;
import openjava.ptree.*;
import openjava.ptree.util.*;

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

    public static final OJClass clazzVoid = OJClass.forClass(
        void.class);

    public static final OJClass clazzObject = OJClass.forClass(
        java.lang.Object.class);

    public static final OJClass clazzObjectArray = OJClass.arrayOf(
        clazzObject);

    public static final OJClass clazzCollection = OJClass.forClass(
        java.util.Collection.class);

    public static final OJClass clazzMap = OJClass.forClass(
        java.util.Map.class);

    public static final OJClass clazzMapEntry = OJClass.forClass(
        java.util.Map.Entry.class);

    public static final OJClass clazzHashtable = OJClass.forClass(
        java.util.Hashtable.class);

    public static final OJClass clazzEnumeration = OJClass.forClass(
        java.util.Enumeration.class);

    public static final OJClass clazzIterator = OJClass.forClass(
        java.util.Iterator.class);

    public static final OJClass clazzIterable = OJClass.forClass(
        org.eigenbase.runtime.Iterable.class);

    public static final OJClass clazzVector = OJClass.forClass(
        java.util.Vector.class);

    public static final OJClass clazzComparable = OJClass.forClass(
        java.lang.Comparable.class);

    public static final OJClass clazzComparator = OJClass.forClass(
        java.util.Comparator.class);

    public static final OJClass clazzResultSet = OJClass.forClass(
        java.sql.ResultSet.class);

    public static final OJClass clazzClass = OJClass.forClass(
        java.lang.Class.class);

    public static final OJClass clazzString = OJClass.forClass(
        java.lang.String.class);

    public static final OJClass clazzSet = OJClass.forClass(
        java.util.Set.class);

    public static final OJClass clazzSQLException = OJClass.forClass(
        java.sql.SQLException.class);

    public static final OJClass clazzEntry = OJClass.forClass(
        java.util.Map.Entry.class);

    public static final OJClass[] emptyArrayOfOJClass = new OJClass[]{};

    static 
    {
        OJSystem.initConstants();
    }
    
    /**
     * Each thread's enclosing {@link OJClass}. Synthetic classes are declared
     * as inner classes of this.
     */
    public static final ThreadLocal threadDeclarers = new ThreadLocal();

    private static ThreadLocal threadTypeFactories = new ThreadLocal();

    //~ Methods ---------------------------------------------------------------

    public static void setThreadTypeFactory(OJTypeFactory typeFactory)
    {
        threadTypeFactories.set(typeFactory);
    }

    public static OJTypeFactory threadTypeFactory()
    {
        return (OJTypeFactory) threadTypeFactories.get();
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
    
    public static String replaceDotWithDollar( String base, int i )
    {
	return base.substring( 0, i ) + '$' + base.substring( i + 1 );
    }

    /**
     * Guesses the row-type of an expression which has type <code>clazz</code>.
     * For example, {@link String}[] --> {@link String}; {@link
     * java.util.Iterator} --> {@link Object}.
     */
    public static final OJClass guessRowType(OJClass clazz)
    {
        if (clazz.isArray()) {
            return clazz.getComponentType();
        } else if (clazzIterator.isAssignableFrom(clazz) ||
            clazzEnumeration.isAssignableFrom(clazz) ||
            clazzVector.isAssignableFrom(clazz) ||
            clazzCollection.isAssignableFrom(clazz) ||
            clazzResultSet.isAssignableFrom(clazz))
        {
            return clazzObject;
        } else if (clazzHashtable.isAssignableFrom(clazz) ||
            clazzMap.isAssignableFrom(clazz))
        {
            return clazzEntry;
        } else {
            return null;
        }
    }

    /**
     * Sets a {@link ParseTreeVisitor} going on a parse tree, and returns the
     * result.
     */
    public static ParseTree go(ParseTreeVisitor visitor, ParseTree p)
    {
        ObjectList holder = new ObjectList(p);
        try {
            p.accept(visitor);
        } catch (StopIterationException e) {
            // ignore the exception -- it was just a way to abort the traversal
        } catch (ParseTreeException e) {
            throw Util.newInternal(
                    e, "while visiting expression " + p);
        }
        return (ParseTree) holder.get(0);
    }

    /**
     * Sets a {@link ParseTreeVisitor} going on a given non-relational
     * expression, and returns the result.
     */
    public static Expression go(ParseTreeVisitor visitor, Expression p)
    {
        return (Expression) go(visitor, (ParseTree) p);
    }

    /**
     * Ensures that an expression is an object.  Primitive expressions are
     * wrapped in a constructor (for example, the <code>int</code> expression
     * <code>2 + 3</code> becomes <code>new Integer(2 + 3)</code>);
     * non-primitive expressions are unchanged.
     *
     * @param exp an expression
     * @param clazz <code>exp</code>'s type
     * @return a call to the constructor of a wrapper class if <code>exp</code>
     *    is primitive, <code>exp</code> otherwise
     **/
    public static Expression box(OJClass clazz, Expression exp)
    {
        if (clazz.isPrimitive()) {
            return new AllocationExpression(
                    clazz.primitiveWrapper(),
                    new ExpressionList(exp));
        } else {
            return exp;
        }
    }

    /**
     * Gets the root environment, which is always a {@link GlobalEnvironment}.
     *
     * @param env environment to start search from
     */
    public static GlobalEnvironment getGlobalEnvironment(Environment env)
    {
	for (;;) {
	    Environment parent = env.getParent();
	    if (parent == null) {
		return (GlobalEnvironment) env;
	    } else {
		env = parent;
	    }
	}
    }

    /**
     * If env is a {@link ClassEnvironment} for declarerName, record new inner
     * class innerName; otherwise, pass up the environment hierarchy.
     *
     * @param env environment to start search from
     * @param declarerName fully-qualified name of enclosing class
     * @param innerName    simple name of inner class
     */
    public static void recordMemberClass(
        Environment env,String declarerName, String innerName)
    {
	do {
            if (env instanceof ClassEnvironment) {
                if (declarerName.equals(env.currentClassName())) {
                    ((ClassEnvironment) env).recordMemberClass(innerName);
                    return;
                }
            } else {
                env = env.getParent();
            }
        } while (env != null);
    }
    
    public static OJClass getType(Environment env, Expression exp)
    {
        try {
            OJClass clazz = exp.getType(env);
            assert(clazz != null);
            return clazz;
        } catch (Exception e) {
            throw Util.newInternal(e, "while deriving type for '" + exp + "'");
        }
    }

    /**
     * A <code>StopIterationException</code> is a way to tell a {@link
     * openjava.ptree.util.ParseTreeVisitor} to halt traversal of the tree, but
     * is not regarded as an error.
     **/
    public static class StopIterationException extends ParseTreeException
    {
        public StopIterationException()
        {
        }
    };
}


// End OJUtil.java
