/**
 * The WhileStatement class presents while statement node
 * of parse tree
 *
 * Jun 20, 1997 by mich
 * Sep 29, 1997 by mich
 * Oct 11, 1997 by mich
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Oct 11, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;


/**
 * The WhileStatement class presents while statement node
 * of parse tree
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Statement
 */
public class WhileStatement extends NonLeaf
    implements Statement, ParseTree
{

    /**
     * Allocates a new object.
     *
     * @param  expr  the expression of the condition of this while
     *               statement.
     * @param  stmts  the statement list of the body oof this while
     *                statement.
     */
    public WhileStatement( Expression expr, StatementList stmts ) {
	super();
	set( (ParseTree) expr, (ParseTree) stmts );
    }

    WhileStatement() {
	super();
    }
  
    public void writeCode() {
	writeTab();  writeDebugL();

	out.print( "while" );

	out.print( " (" );
	Expression expr = getExpression();
	expr.writeCode();
	out.print( ")" );

	StatementList stmts = getStatements();

	if (stmts.isEmpty()) {
	    out.print( " ;" );
	} else {
	    out.println( " {" );
	    pushNest();  stmts.writeCode();  popNest();
	    writeTab();  out.print( "}" );
	}

	writeDebugR();  out.println();
    }

    /**
     * Gets the condtion of this while statement.
     *
     * @return  the expression of the condtion.
     */
    public Expression getExpression() {
	return (Expression) elementAt( 0 );
    }

    /**
     * Sets the condtion of this while statement.
     *
     * @param  expr  the expression of the condtion to set.
     */
    public void setExpression( Expression expr ) {
	setElementAt( expr, 0 );
    }
  
    /**
     * Gets the body of this while statement.
     *
     * @return  the statement list of the body.
     */
    public StatementList getStatements() {
	return (StatementList) elementAt( 1 );
    }

    /**
     * Sets the body of this while statement.
     *
     * @param  stmts  the statement list of the body to set.
     */
    public void setStatements( StatementList stmts ) {
	setElementAt( stmts, 1 );
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}
