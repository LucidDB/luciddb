/*
 * CatchBlock.java 1.0
 *
 *
 * Jun 20, 1997 by mich
 * Sep 29, 1997 by bv
 * Oct 10, 1997 by mich
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Oct 10, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;



/**
 * The CatchBlock class presents catch node of parse tree
 *
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Parameter
 * @see openjava.ptree.StatementList
 */
public class CatchBlock extends NonLeaf
{
    /**
     * Allocates a new CatchBlock object.
     *
     * @param  typespec  the exception type specifier.
     * @param  name  the exception variable name.
     * @param  stmts  the statement list of the body.
     */
    public CatchBlock(Parameter  param, StatementList stmts) {
	super();
	if(stmts == null) {
	    stmts = new StatementList();
	}
	set(param, stmts);
    }

    CatchBlock() {
	super();
    }
  
    public void writeCode() {
	out.print( " catch" );
	
	out.print( " ( " );
	
	Parameter param = getParameter();
	param.writeCode();
	
	out.print( " )" );
	
	out.println( " {" );
	
	StatementList bl = getBody();
	pushNest();  bl.writeCode();  popNest();
	
	writeTab();  out.print( "}" );
    }
  
    /**
     * Gets the exception parameter of this catch block.
     *
     * @return  the exception parameter.
     */
    public Parameter getParameter() {
	return (Parameter) elementAt( 0 );
    }

    /**
     * Sets the exception parameter of this catch block.
     *
     * @param  tspec  the exception parameter.
     */
    public void setParameter( Parameter param ) {
	setElementAt( param, 0 );
    }
  
    /**
     * Gets the body of this catch block.
     *
     * @return  the statement list of the body.
     */
    public StatementList getBody() {
	return (StatementList) elementAt( 1 );
    }

    /**
     * Sets the body of this catch block.
     *
     * @param  stmts  the statement list of the body.
     */
    public void setBody( StatementList stmts ) {
	setElementAt( stmts, 1 );
    }

    public void accept(ParseTreeVisitor v) throws ParseTreeException {
        v.visit(this);
    }

}
