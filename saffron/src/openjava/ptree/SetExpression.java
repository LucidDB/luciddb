/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Tech
// You must accept the terms in LICENSE.html to use this software.
*/

package openjava.ptree;
import openjava.mop.*;
import java.util.Vector;

/**
 * A <code>SetExpression</code> is an expression which consists of a set of
 * rows.  (Other expressions, such as variable references, may have set types
 * also.)
 */
public abstract class SetExpression extends NonLeaf
	implements Expression
{
    private OJClass rowType;

    // implement Expression
    public synchronized OJClass getRowType(Environment env)
    {
	if (rowType == null) {
	    try {
		rowType = deriveRowType(env);
	    } catch (Exception e) {
            throw Toolbox.newInternal(
                e,
                "unexpected exception getting row type for SetExpression");
	    }
	}
	return rowType;
    }

    protected abstract OJClass deriveRowType(Environment env)
	throws Exception;

    // implement Expression
    public OJClass getType(Environment env)
    {
	OJClass rowType = getRowType(env);
	return OJClass.arrayOf(rowType);
//  	try {
//  	    return OJClass.forName(rowType.getName() + "[]");
//  	} catch (OJClassNotFoundException e) {
//  	    throw Toolbox.newInternal(e);
//  	}
    }

    public Expression[] flatten()
    {
	return flatten(this);
    }

    public static Expression[] flatten(Expression expression)
    {
	Vector v = new Vector();
	if (expression != null) {
	    flattenRecurse(expression, v);
	}
	Expression[] expressions = new Expression[v.size()];
	v.copyInto(expressions);
	return expressions;
    }

    private static void flattenRecurse(Expression expression, Vector v)
    {
	if (expression instanceof JoinExpression) {
	    JoinExpression join = (JoinExpression) expression;
	    flattenRecurse(join.getLeft(), v);
	    flattenRecurse(join.getRight(), v);
	} else {
	    v.addElement(expression);
	}
    }
};

// End SetExpression.java
