/*
 * ContinueStatement.java 1.0
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
 * The <code>ContinueStatement</code> class represents
 * a continue statement node of parse tree.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Statement
 */
public class ContinueStatement extends NonLeaf
    implements Statement
{

    /**
     * Allocates a new ContinueStatement object.
     *
     * @param  label  the label name.
     */
    public ContinueStatement( String label ) {
	super();
	set( label );
    }

    /**
     * Allocates a new ContinueStatement object.
     *
     */
    public ContinueStatement() {
	this( null );
    }
  
    public void writeCode() {
	writeTab();  writeDebugL();

	out.print( "continue" );

	String label = getLabel();
	if (label != null) {
	    out.print( " " );
	    out.print( label );
	}

	out.print( ";" );

	writeDebugR();  out.println();
    }
  
    /**
     * Gets the label of this break statement.
     *
     * @return  the label name.
     *          If there is no label then this method returns null.
     */
    public String getLabel() {
	return (String) elementAt( 0 );
    }

    /**
     * Sets the label of this break statement.
     *
     * @param  label  the label.
     */
    public void setLabel( String label ) {
	setElementAt( label, 0 );
    }

    public void accept(ParseTreeVisitor v) throws ParseTreeException {
        v.visit(this);
    }

}
