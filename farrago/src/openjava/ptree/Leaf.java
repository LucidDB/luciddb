/*
 * Leaf.java 1.0
 *
 * This subclass of symbol represents (at least) terminal symbols returned 
 * by the scanner and placed on the parse stack.  At present, this 
 * class does nothing more than its super class.
 * 
 * Jun 11, 1997
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.ParseTreeObject
 * @version 1.0 last updated: Jun 11, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;
import openjava.mop.Environment;


/**
 * The Leaf class is a token-node in the parse tree of OpenJava.
 * Any object of this class or subclasses must be immutable.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 */
public class Leaf extends ParseTreeObject
    implements ParseTree
{
    protected void replaceChildWith( ParseTree dist, ParseTree replacement )
        throws ParseTreeException
    {
	throw new ParseTreeException( "no child" );
    }

    /** textString is the text of this token. */
    private String textString = null;
  
    /** tokenID is the identifer number of this token */
    private int tokenID = 0;
  
    /** line is the number of the line at which this token is. */
    public int line = -1;
  
    /** charBegin is the number of the character at which this token is. */
    public int charBegin = -1;
  
    /**
     * Allocates a new leaf(token) with its text.
     *
     * @param  term_num  dummy parameter.
     * @param  str  its text.
     */
    public Leaf( String str ) {
        this( -1, str, -1, -1 );
    }
  
    /**
     * Allocates a new leaf(token) with its text.
     *
     * @param  term_num  dummy parameter.
     * @param  str  its text.
     */
    public Leaf( int term_num, String str ) {
        this( term_num, str, -1, -1 );
    }
  
    /**
     * Allocates a new leaf(token) with its text and where this is.
     *
     * @param  term_num  dummy parameter.
     * @param  str  its text.
     */
    public Leaf( int term_num, String str, int line, int charBegin ) {
        tokenID = term_num;
	textString = str;
	this.line = line;
	this.charBegin = charBegin;
    }
  
    /**
     * Overrides to return its text as the string of this instance.
     *
     * @return  the text of this token.
     */
    public String toString() {
        return textString;
    }
  
    /**
     * Makes a new copy of this leaf-node.
     * This method equals to makeCopy().
     *
     * @return  the copy of this nonleaf-node as a ptree-node.
     */
    public ParseTree makeRecursiveCopy() {
      return (ParseTree) this.clone() ;
    }
  
    /**
     * Makes a new copy of this leaf-node.
     *
     * @return  the copy of this nonleaf-node as a ptree-node.
     */
    public ParseTree makeCopy() {
        return (ParseTree) this.clone () ;
    }
  
    /**
     * Tests if the specified ptree-node equals to this leaf-node.
     *
     * @param  p  the ptree-node to be tested.
     * @return  true if p equals to this leaf-node
     */
    public boolean equals( ParseTree p ) {
        if (p == null)  return false;
	if (!(p instanceof Leaf))  return false;
	if (eq( this, p ))  return true;
	return this.toString().equals( p.toString() );
    }

    /**
     * Tests if the specified string equals to this leaf-node's text.
     *
     * @param  p  the ptree-node to be tested.
     * @return  true if p equals to this leaf-node
     */
    public boolean equals( String str ) {
        if(str == null)  return false;
	return this.toString().equals( str );
    }
  
    /**
     * Returns the identifer-number of this token.
     *
     * @return  the identifer-number of this token.
     */
    public int getTokenID() {
        return tokenID;
    }
  
    /**
     * Writes the code of this token.
     *
     * @return  always true.
     */
    public void writeCode() {
        out.print( toString() );
    }
  
    /**
     * Accepts a <code>ParseTreeVisitor</code> object as the role of a
     * Visitor in the Visitor pattern, as the role of an Element in the
     * Visitor pattern.<p>
     *
     * This invoke an appropriate <code>visit()</code> method on each
     * child <code>ParseTree</code> object with this visitor.
     *
     * @param visitor a visitor
     **/
    public void childrenAccept( ParseTreeVisitor visitor ) {
	return;
    }

    public void accept(ParseTreeVisitor v) throws ParseTreeException {
	v.visit(this);
    }

    // default implementation of Expression -- derived classes, please do
    // better
    public OJClass getRowType( Environment env ) throws Exception
    {
	if (this instanceof Expression) {
	    OJClass clazz = ((Expression) this).getType( env );
	    return Toolbox.guessRowType(clazz );
	} else {
	    // only Expressions have row-types -- you shouldn't have asked
	    throw new UnsupportedOperationException();
	}
    }
}
