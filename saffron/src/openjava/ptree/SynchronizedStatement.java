/*
 * Catch.java 1.0
 *
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
 * The <code>SynchronizedStatement</code> class represents
 * a synchronized statement node of parse tree.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Statement
 */
public class SynchronizedStatement extends NonLeaf
    implements Statement, ParseTree
{
    /**
     * Allocates a new object.
     *
     * @param  expr  the expression to be synchronized.
     * @param  stmts  the statements guarded by this synchronization.
     */
    public SynchronizedStatement(Expression expr, StatementList  stmts) {
	super();
	set( (ParseTree) expr, (ParseTree) stmts );
    }

    SynchronizedStatement() {
	super();
    }
    
    public void writeCode() {
	writeTab();  writeDebugL();

	out.print( "synchronized" );

	out.print( " ( " );
	Expression expr = getExpression();
	expr.writeCode();
	out.println( " )" );

	StatementList stmts = getStatements();
	if(stmts.isEmpty()){
	    out.print( " ;" );
	}else{
	    out.println( " {" );
	    pushNest();  stmts.writeCode();  popNest();
	    writeTab(); out.print( "}" );
	}

	writeDebugR();  out.println();
    }
    
    /**
     * Gets the expression to be synchronized.
     *
     * @return  the expression to be synchronized.
     */
    public Expression getExpression() {
	return (Expression) elementAt( 0 );
    }
    
    /**
     * Sets the expression to be synchronized by this statement.
     *
     * @param  expr  the expression to be synchronized by this statement.
     */
    public void setExpression( Expression expr ) {
	setElementAt( expr, 0 );
    }
    
    /**
     * Gets the statements guarded by this synchronization.
     *
     * @return  the statements guarded by this synchronization.
     */
    public StatementList getStatements() {
	return (StatementList) elementAt( 1 );
    }
    
    /**
     * Sets the statements guarded by this synchronization.
     *
     * @param  stmts  the statements guarded by this synchronization.
     */
    public void setStatements( StatementList stmts ) {
	setElementAt( stmts, 1 );
    }
    
    public void accept(ParseTreeVisitor v) throws ParseTreeException {
        v.visit(this);
    }

}
