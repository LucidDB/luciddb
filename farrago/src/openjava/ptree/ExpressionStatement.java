/*
 * ExpressionStatement.java 1.0
 *
 * Jun 20, 1997 mich
 * Sep 29, 1997 bv
 * Oct 11, 1997 mich
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Oct 11, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;



/**
 * The ExpressionStatement class presents expression statement node
 * of parse tree
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Statement
 * @see openjava.ptree.Expression
 */
public class ExpressionStatement extends NonLeaf
    implements Statement
{
    /**
     * Allocates a new object.
     *
     * @param statement prototype object
     */
    public ExpressionStatement( Expression expr ) {
	super();
	set( (ParseTree) expr );
    }
  
    ExpressionStatement() {
	super();
    }
    
    public void writeCode() {
	writeTab();  writeDebugL();

	Expression expr = getExpression();
	expr.writeCode();

	out.print(";");

	writeDebugR();  out.println();
    }

    /**
     * Gets the expression of this statement.
     *
     * @return  the expression.
     */
    public Expression getExpression() {
	return (Expression) elementAt( 0 );
    }
  
    /**
     * Sets the expression of this statement.
     *
     * @param  the expression to set.
     */
    public void setExpression( Expression expr ) {
	setElementAt( expr, 0 );
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}
