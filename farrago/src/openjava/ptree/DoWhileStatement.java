/*
 * DoWhileStatement.java 1.0
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
 * The <code>DoWhileStatement</code> class represents a do-while
 * statement node of parse tree.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Statement
 * @see openjava.ptree.Expression
 */
public class DoWhileStatement extends NonLeaf
    implements Statement, ParseTree
{
    /**
     * Allocates a new object.
     *
     * @param  stmts  the statement list of the body.
     * @param  expr  the expression of the condition.
     */
    public DoWhileStatement(StatementList  stmts, Expression expr) {
	super();
	set((ParseTree) stmts, (ParseTree) expr);
    }

    DoWhileStatement() {
	super();
    }
  
    public void writeCode() {
	writeTab();  writeDebugL();
	
	out.print( "do" );
	
	StatementList stmts = getStatements();
	
	if (stmts.isEmpty()) {
	    out.print( " ; " );
	} else {
	    out.println( " {" );
	    pushNest();  stmts.writeCode();  popNest();
	    writeTab();  out.println( "} " );
	}
	
	out.print( "while" );
	
	out.print( " (" );
	Expression expr = getExpression();
	expr.writeCode();
	out.print( ")" );
	
	out.print(";");
	
	writeDebugR();  out.println();
    }
    
    /**
     * Gets the body of this do-while statement.
     *
     * @return  the statement list of the body.
     */
    public StatementList getStatements() {
	return (StatementList) elementAt( 0 );
    }
    
    /**
     * Sets the body of this do-while statement.
     *
     * @param  stmts  the statement list of the body to set.
     */
    public void setStatements( StatementList stmts ) {
	setElementAt( stmts, 0 );
    }
    
    /**
     * Gets the condtion of this do-while statement.
     *
     * @return  the expression of the condtion.
     */
    public Expression getExpression() {
	return (Expression) elementAt( 1 );
    }
    
    /**
     * Sets the condtion of this do-while statement.
     *
     * @param  expr  the expression of the condtion to set.
     */
    public void setExpression( Expression expr ) {
	setElementAt( expr, 1 );
    }
    
    public void accept(ParseTreeVisitor v) throws ParseTreeException {
        v.visit(this);
    }

}
