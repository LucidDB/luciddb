/*
 * List.java 1.0
 * 
 * Jun 11, 1997 by mich
 * Aug 20, 1997 by mich
 * Sep 28, 1997 by mich
 * 
 * @version 1.0 last updated: Sep 28, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import java.io.PrintStream;
import java.util.*;
import openjava.ptree.util.*;
import openjava.tools.DebugOut;


/**
 * The List class presents for the list of parse trees.
 *
 */
public abstract class List extends ParseTreeObject
implements ParseTree
{

    protected final void
    replaceChildWith( ParseTree dist, ParseTree replacement )
        throws ParseTreeException
    {
	DebugOut.println( "List.replaceChildWith() [" + dist +
			  "] with [" + replacement + "]");
	for (int i = 0, size = contents.size(); i < size; ++i) {
	    if (contents_elementAt( i ) == dist) {
		contents_setElementAt( replacement, i );
		return;
	    }
	}
	throw new ParseTreeException( "no replacing target" );
    }
  
    /** The ptreelist  the list of parse-tree nodes */
    private Vector contents = new Vector();
    protected void contents_addElement( Object obj ) {
	contents.addElement( obj );
	if (obj instanceof ParseTreeObject) {
	    ((ParseTreeObject) obj).setParent( this );
	}
    }
    protected void contents_insertElementAt( Object obj, int index ) {
	contents.insertElementAt( obj, index );
	if (obj instanceof ParseTreeObject) {
	    ((ParseTreeObject) obj).setParent( this );
	}
    }
    protected void contents_setElementAt( Object obj, int index ) {
	contents.setElementAt( obj, index );
	if (obj instanceof ParseTreeObject) {
	    ((ParseTreeObject) obj).setParent( this );
	}
    }
    protected Object contents_elementAt( int index ) {
	return contents.elementAt( index );
    }
    protected void contents_removeElementAt( int index ) {
	contents.removeElementAt( index );
    }
    protected int contents_size() {
	return contents.size();
    }
  
    private String delimiter = ParseTreeObject.LN;
  
    /**
     * Allocates this List
     *
     */
    protected List() {
	contents = new Vector();
    }
  
    /**
     * Allocates this List
     *
     * @param p0  list's element
     */
    protected List( Object p ) {
	this();
	contents_addElement( p );
    }
  
    /**
     * Allocates this List
     *
     */
    protected List( String delimiter ) {
	this();
	this.delimiter = delimiter;
    }

    /**
     * Allocates this List
     *
     * @param p0  list's element
     */
    protected List( String delimiter, Object p ) {
	this( delimiter );
	contents_addElement( p );
    }

    /**
     * Get contents
     */
    public Enumeration elements() {
	return contents.elements();
    }
  
    /**
     * Returns the length of this list.
     *
     * @return  the length of this list
     */
    public int size() {
	return contents.size();
    }
  
    /**
     * Tests if this list is empty.
     *
     * @return  true if this list is empty
     */
    public boolean isEmpty() {
	return contents.isEmpty();
    }
  
    /**
     * Removes an element at the specified point from this list
     *
     * @param  n  where to remove element.
     */
    public void removeAll() {
	contents.removeAllElements();
    }

    /**
     * Tests if any element representing the specified string is exist
     * or not.
     *
     * @param  str  a string to test.
     * @return  true if any element representing the specified string
     *		is exist.
     */
    public boolean contains( String str ) {
	Enumeration it = contents.elements();
	while (it.hasMoreElements()) {
	    Object elem = it.nextElement();
	    if (elem != null && elem.toString().equals( str ))  return true;
	}
	return false;
    }
  
    /**
     * Tests if this list-node's value equals to the specified
     * ptree-node's.
     *
     * @return  true if two values are same.
     */
    public boolean equals( ParseTree p ) {
	if (p == null)  return false;
	if (this == p)  return true;
	if(this.getClass() != p.getClass())  return false;
  
	List nlp = (List) p;
	int length = this.size();
	if (nlp.size() != length)  return false;
	for (int i = 0; i < length; ++i) {
	    Object a = contents.elementAt( i );
	    Object b = nlp.contents.elementAt( i );
	    if (a != null && b == null)  return false;
	    if (a == null && b != null)  return false;
	    if (a == null && b == null)  continue;
	    if (! a.equals( b ))  return false;
	}
	return true;
    }
  
    /**
     * Makes a new copy (another object) of this list-node.
     * The objects contained by this object will also be copied
     *
     * @return  the copy of this nonleaf-node as a ptree-node.
     */
    public ParseTree makeRecursiveCopy() {
        List result = (List) clone();
	result.contents = new Vector();

	Enumeration it = contents.elements();
	while (it.hasMoreElements()) {
	    Object elem = it.nextElement();
	    if (elem instanceof ParseTree) {
		elem = ((ParseTree) elem).makeRecursiveCopy();
	    }
	    result.contents_addElement( elem );
	}
  
	return result;
    }
  
    /**
     * Writes the code this parse-tree presents for.
     *
     */
    public void writeCode() {
  
	if (isEmpty())  return;
  
	Object elem = (ParseTree) contents.elementAt( 0 );
	if (elem == null) {
	    writeDebug( "#null" );
	} else if (elem instanceof ParseTree) {
	    ((ParseTree) elem).writeCode();
	} else {
	    out.print( elem.toString() );
	}
	for (int i = 1, len = size(); i < len; ++i) {
	    out.print( this.delimiter );
	    elem = contents.elementAt( i );
	    if (elem == null) {
		writeDebug( "#null" );
	    } else if (elem instanceof ParseTree) {
		((ParseTree) elem).writeCode();
	    } else {
		out.print( elem.toString() );
	    }
	}
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
    public void childrenAccept( ParseTreeVisitor visitor )
	throws ParseTreeException
    {
        if (contents == null)  return;
	int length = contents.size();
        for (int i = 0; i < length; ++i) {
	    Object obj = contents.elementAt( i );
            if (obj instanceof ParseTree) {
                ParseTree ptree = (ParseTree) obj;
                ptree.accept( visitor );
            }
        }
    }

}
