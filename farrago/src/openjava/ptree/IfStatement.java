/*
 * IfStatement.java 1.0
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
 * The <code>IfStatement</code> class represents a if statement node
 * of parse tree
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Statement
 * @see openjava.ptree.Expression
 * @see openjava.ptree.StatementList
 */
public class IfStatement extends NonLeaf
    implements Statement, ParseTree
{

    /**
     * Constructs new IfStatement from prototype object
     *
     * @param  expr  the condition of this if statement.
     * @param  stmts  the statement that is executed when expr is ture
     * @param  elsestmts the statement that is executed when expr is false.
     *                   If there is no else part then statement list is
     *                   empty.
     */
    public IfStatement(
	Expression     expr,
	StatementList  stmts,
	StatementList  elsestmts )
    {
	super();
	if (stmts == null)  stmts = new StatementList();
	if (elsestmts == null)  elsestmts = new StatementList();
	set( expr, stmts, elsestmts );
    }

    /**
     * Constructs new IfStatement from prototype object
     *
     * @param  expr  the condition of this if statement.
     * @param  stmts  the statement that is executed when expr is ture
     */
    public IfStatement( Expression expr, StatementList stmts ) {
	this( expr, stmts, null );
    }
  
    IfStatement() {
	super();
    }

    public void writeCode() {
	writeTab();  writeDebugL();

	out.print( "if" );

	out.print( " (" );
	Expression expr = getExpression();
	expr.writeCode();
	out.print( ")" );
	
	/* then part */
	out.println( " {" );
	StatementList stmts = getStatements();
	pushNest();  stmts.writeCode();  popNest();
	writeTab();  out.print( "}" );  
    
	/* else part */
	StatementList elsestmts = getElseStatements();
	if(! elsestmts.isEmpty()){
	    out.print( " else " );
	    out.println( "{" );
	    pushNest();  elsestmts.writeCode();  popNest();
	    writeTab();  out.print( "}" );
	}

	writeDebugR();  out.println();
    }

    /**
     * Gets the condition of this if statement.
     *
     * @return  the expression of the condition.
     */
    public Expression getExpression() {
	return (Expression) elementAt( 0 );
    }

    /**
     * Sets the condition of this if statement.
     *
     * @param  expr  the expression of the condition.
     */
    public void setExpression( Expression expr ) {
	setElementAt( expr, 0 );
    }
  
    /**
     * Gets the then part of this if statement.
     *
     * @return  the statement list of the then part.
     */
    public StatementList getStatements() {
	return (StatementList) elementAt( 1 );
    }

    /**
     * Sets the then part of this if statement.
     *
     * @param  thenstmts  the statement list of the then part.
     */
    public void setStatements( StatementList thenstmts ) {
	setElementAt( thenstmts, 1 );
    }
  
    /**
     * Gets the else part of this if statement.
     *
     * @return  the statement list of the else part.
     */
    public StatementList getElseStatements() {
	return (StatementList) elementAt(2);
    }

    /**
     * Sets the else part of this if statement.
     *
     * @param  elsestmts  the statement list of the else part.
     */
    public void setElseStatements( StatementList elsestmts ) {
	setElementAt( elsestmts, 2 );
    }
  
    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}
