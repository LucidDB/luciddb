/*
 * TryStatement.java 1.0
 *
 * Jun 20, 1997 mich
 * Sep 29, 1997 mich
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Sep 29, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;



/**
 * The <code>TryStatement</code> class represents
 * a try statement node of parse tree.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Statement
 * @see openjava.ptree.StatementList
 * @see openjava.ptree.CatchList
 */
public class TryStatement extends NonLeaf
    implements Statement, ParseTree
{

    /**
     * Allocates a new TryStatement object.
     *
     * @param  the statement list of the body of this try statement.
     * @param  the catch block list of this try statement.
     * @param  the statement list of the finally block.
     */
    public TryStatement(
	StatementList  stmts,
	CatchList      catchlist,
	StatementList  finallee )
    {
        super();
	if (stmts == null)  stmts = new StatementList();
	if (catchlist == null)  catchlist = new CatchList();
	if (finallee == null)  finallee = new StatementList();
	set( stmts, catchlist, finallee );
    }

    /**
     * Allocates a new TryStatement object.
     *
     * @param  the statement list of the body of this try statement.
     * @param  the catch block list of this try statement.
     */
    public TryStatement( StatementList  stmts, CatchList      catchlist ) {
        this( stmts, catchlist, new StatementList() );
    }

    /**
     * Allocates a new TryStatement object.
     *
     * @param  the statement list of the body of this try statement.
     * @param  the statement list of the finally block.
     */
    public TryStatement( StatementList  stmts, StatementList  finallee ) {
        this( stmts, new CatchList(), finallee );
    }

    TryStatement() {
    }

    public void writeCode() {
        writeTab();  writeDebugL();
	out.print( "try" );
	out.println( " {" );
	
	StatementList stmtlist = getBody();
	if (! stmtlist.isEmpty()) {
	    pushNest();  stmtlist.writeCode();  popNest();
	}

	writeTab();  out.print( "}" );
    
	CatchList catchlist = getCatchList();
	if (! catchlist.isEmpty()) {
	    catchlist.writeCode();
	}
	
	StatementList finstmt = getFinallyBody();
	if (! finstmt.isEmpty()) {
	    out.println( " finally {" );
	    pushNest();  finstmt.writeCode();  popNest();
	    writeTab();  out.print( "}" );
	}

	writeDebugR();  out.println();
    }
  
    /**
     * Gets the body of this try statement.
     *
     * @return  the statement list of the body.
     */
    public StatementList getBody() {
        return (StatementList) elementAt( 0 );
    }

    /**
     * Sets the body of this try statement.
     *
     * @param  stmts  the statement list of the body to set.
     */
    public void setBody( StatementList stmts ) {
        setElementAt( stmts, 0 );
    }
  
    /**
     * Gets the catch block list.
     *
     * @return  the catch block list.
     */
    public CatchList getCatchList() {
        return (CatchList) elementAt( 1 );
    }

    /**
     * Sets the catch block list.
     *
     * @param  catchlist  the catch block list.
     */
    public void setCatchList( CatchList catchlist ) {
        setElementAt( catchlist, 1 );
    }
    
    /**
     * Gets the finally body.
     *
     * @return  the statement list of finally body.
     */
    public StatementList getFinallyBody() {
        return (StatementList) elementAt( 2 );
    }

    /**
     * Sets the finally body.
     *
     * @param  finallee  the statement list of finally body.
     */
    public void setFinallyBody( StatementList finallee ) {
        setElementAt( finallee, 2 );
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}
