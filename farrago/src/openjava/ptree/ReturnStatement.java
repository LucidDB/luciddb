/*
 * ReturnStatement.java 1.0
 *
 *
 * Jun 20, 1997 by mich
 * Sep 29, 1997 by bv
 * Oct 11, 1997 by mich
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
 * The <code>ReturnStatement</code> class represents
 * a return statement node of parse tree.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Statement
 */
public class ReturnStatement extends NonLeaf
    implements Statement, ParseTree
{
    /**
     * Allocates a new object.
     *
     * @param  expr  the expression to be returned by this statement.
     *               If this is null, nothing to be returned.
     */
    public ReturnStatement( Expression expr ) {
	super();
	set( (ParseTree) expr );
    }
  
    /**
     * Allocates a new object.
     *
     */
    public ReturnStatement() {
      this( null );
    }
    
    public void writeCode() {
        writeTab();  writeDebugL();
    
        out.print( "return" );
    
        Expression expr = getExpression();
        if(expr != null){
          out.print(" ");
          expr.writeCode();
        }
    
        out.print(";");
    
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
