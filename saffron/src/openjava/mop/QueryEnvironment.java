/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
*/

package openjava.mop;
import openjava.ptree.Expression;
import openjava.ptree.QueryExpression;
import openjava.ptree.SetExpression;

/**
 * <code>QueryEnvironment</code> is the environment seen inside a {@link
 * QueryExpression}.  The 'variables' are the tables in the from list; it
 * inherits stuff from the enclosing java environment, plus from enclosing
 * queries.  TBD: can queries in a from list inherit?
 */
public class QueryEnvironment extends ClosedEnvironment {
    private QueryExpression query;

    public QueryEnvironment(Environment e, QueryExpression query)
    {
	super(e);
	this.query = query;
    }

    // implement Environment
    public VariableInfo lookupBind(String name) {
	Expression[] expressions = SetExpression.flatten(query.getFrom());
	for (int i = 0; i < expressions.length; i++) {
	    String alias = Toolbox.getAlias(expressions[i]);
	    if (alias != null && alias.equals(name)) {
		try {
			return new BasicVariableInfo(expressions[i].getRowType(parent));
		} catch (Exception e) {
            throw Toolbox.newInternal(
                e,
                "unexpected exception looking up " + name
                + " in QueryEnvironment");
		}
	    }
	}
	return parent.lookupBind(name);
    }

    // implement Environment
    public boolean isBind(String name)
    {
	Expression expression = lookupFrom(name);
	if (expression != null) {
	    return true;
	}
	if (parent != null) {
	    return parent.isBind(name);
	}
	return false;
    }

    /**
     * Looks for a from-list item called <code>name</code>.
     *
     * @param name name of from-list item
     * @param offset writes the zero-based offset to <code>offset[0]</code> if
     *     the item is found
     * @return the expression if found, null if not
     */
    public Expression lookupFrom(String name, int[] offset) {
	Expression[] expressions = SetExpression.flatten(query.getFrom());
	for (int i = 0; i < expressions.length; i++) {
	    Expression expression = expressions[i];
	    String alias = Toolbox.getAlias(expression);
	    if (alias != null && alias.equals(name)) {
		if (offset != null) {
		    offset[0] = i;
		}
		return expression;
	    }
	}
	return null; // not found
    }

    public Expression lookupFrom(String name) {
	return lookupFrom(name, null);
    }

    public boolean isAggregating()
    {
	return query.getGroupList() != null;
    }
}


// End QueryEnvironment.java
