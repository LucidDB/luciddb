/*
 * ThrowStatement.java 1.0
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
 * The <code>ThrowStatement</code> class represents
 * a throw statement node of parse tree.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Statement
 */
public class ThrowStatement extends NonLeaf
    implements Statement
{

    /**
     * Allocates a new ThrowStatement object.
     *
     * @param  expr  the expression to be throwed by this statement.
     */
    public ThrowStatement( Expression expr ) {
	set( (ParseTree) expr );
    }

    ThrowStatement() {
    }
  
    public void writeCode() {
	writeTab();  writeDebugL();
	
	out.print( "throw " );
	
	Expression expr = getExpression();
	expr.writeCode();
	
	out.print( ";" );
	
	writeDebugR();  out.println();
    }

    /**
     * Gets the returned expression by this statement.
     *
     * @return  the expression to be returned by this statement.
     */  
    public Expression getExpression() {
	return (Expression) elementAt( 0 );
    }

    /**
     * Sets the returned expression by this statement.
     *
     * @param  expr  the expression to be returned by this statement.
     */  
    public void setExpression( Expression expr ) {
	setElementAt( expr, 0 );
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}
