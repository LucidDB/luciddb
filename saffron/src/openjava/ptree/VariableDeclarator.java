/*
 * VariableDeclarator.java 1.0
 *
 * This interface is made to type ptree-node into the variable
 * declarator in the body of class.
 *
 * Jun 20, 1997 by mich
 * Aug 20, 1997 by mich
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
 * The VariableDeclarator class presents variable declarator node
 * of parse tree
 * 
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.VariableInitializer
 */
public class VariableDeclarator extends NonLeaf
{
    int dims = 0;

    /**
     * Allocates a new object.
     *
     * @param  declname  the declarator name.
     * @param  varinit  the variable initializer.
     */
    public VariableDeclarator(
	String declname,
	int dims,		      
	VariableInitializer varinit )
    {
	super();
	set( declname, varinit );
	this.dims = dims;
    }

    /**
     * Allocates a new object.
     *
     * @param  declname  the declarator name.
     * @param  varinit  the variable initializer.
     */
    public VariableDeclarator( String declname, VariableInitializer varinit ) {
	this( declname, 0, varinit );
    }

    VariableDeclarator() {
	super();
    }
  
    /**
     * write code
     *
     * String "=" VaraiableInitializer
     */
    public void writeCode() {
	String declname = getVariable();
	out.print( declname );
	for (int i = 0; i < this.getDimension(); ++i) {
	    out.print( "[]" );
	}

	VariableInitializer varinit = getInitializer();
	if (varinit != null) {
	    out.print( " = " );
	    varinit.writeCode();
	}
    }
  
    /**
     * Gets declarator name, declarator name includes variable name
     * but its dimension.
     * 
     * @return declarator name
     */
    public String getVariable() {
	return (String) elementAt( 0 );
    }

    /**
     * Sets declarator name, declarator name includes variable name
     * but its dimension.
     * 
     * @param  name  declarator name to set.
     * @see  openjava.ptree.TypeName
     */
    public void setVariable( String name ) {
	setElementAt( name, 0 );
    }

    public int getDimension() {
	return this.dims;
    }

    public String dimensionString() {
	StringBuffer buf = new StringBuffer();
	for (int i = 0; i < getDimension(); ++i) {
	    buf.append( "[]" );
	}
	return buf.toString();
    }
  
    /**
     * Gets variable initializer.
     *
     * @return variable initializer.
     */
    public VariableInitializer getInitializer() {
	return (VariableInitializer) elementAt( 1 );
    }

    /**
     * Sets variable initializer.
     *
     * @param  vinit  the variable initializer to set.
     */
    public void setInitializer( VariableInitializer vinit ) {
	setElementAt( vinit, 1 );
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}
