/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
*/

package openjava.ptree;
import openjava.mop.Environment;
import openjava.mop.OJClass;
import java.io.PrintWriter;

public class AliasedExpression extends NonLeaf
    implements Expression
{
    String alias;

    public AliasedExpression(Expression expr, String alias)
    {
	set(expr);
	this.alias = alias;
    }

    public void accept(openjava.ptree.util.ParseTreeVisitor v)
	throws ParseTreeException
    {
        v.visit(this);
    }

    public Expression getExpression()
    {
	return (Expression) elementAt(0);
    }

    // implement Expression
    public OJClass getType(Environment env) throws Exception
    {
	Expression expr = getExpression();
	return expr.getType(env);
    }

    // implement Expression
    public OJClass getRowType(Environment env) throws Exception
    {
	Expression expr = getExpression();
	return expr.getRowType(env);
    }

    public String getAlias()
    {
	return alias;
    }
    
    public void setAlias(String alias)
    {
	this.alias = alias;
    }
};

// End AliasedExpression.java
