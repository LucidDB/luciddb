/*
 * NonLeaf.java 1.0
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
import openjava.tools.DebugOut;
import java.io.OutputStream;
import java.io.Writer;
import java.io.PrintWriter;
import java.io.StringWriter;
import openjava.mop.Environment;


/**
 * The NonLeaf class presents for node of parse tree.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.Leaf
 */
public abstract class NonLeaf extends ParseTreeObject
        implements ParseTree
{
    private String comment = "";

    protected final void
    replaceChildWith( ParseTree dist, ParseTree replacement )
        throws ParseTreeException
    {
	DebugOut.println( "NonLeaf.replaceChildWith() [" + dist +
			  "] with [" + replacement + "]");
	for (int i = 0; i < contents.length; ++i) {
	    if (contents[i] == dist) {
		contents[i] = replacement;
		((ParseTreeObject) replacement).setParent( this );
		return;
	    }
	}
	throw new ParseTreeException("no replacing target " + dist
				     + " for " + replacement
				     + " in the source code : "
				     + toString());
    }

    /** contents is the array of the elements of this nonleaf-node. */
    private Object[] contents = null ;

    /**
     * Allocates a new non-leaf(cons-cell), where the first element has
     * null and the second element has null.
     * 
     */
    public NonLeaf() {
	contents = new Object[0];
    }

    /**
     * Makes a new copy (another object) of this nonleaf-node recursively.
     * The objects contained by this object will also be copied.
     *
     * @return  the copy of this nonleaf-node as a ptree-node.
     */
    public ParseTree makeRecursiveCopy() {
        NonLeaf result = (NonLeaf) makeCopy();

	Object newc[] = new Object[contents.length];
	for (int i = 0; i < contents.length; ++i) {
	    if (contents[i] instanceof ParseTree) {
		ParseTree src = (ParseTree) contents[i];
		newc[i] = (ParseTree) src.makeRecursiveCopy();
	    } else if (contents[i] instanceof String[]) {
		String[] srcary = (String[]) contents[i];
		String[] destary = new String[srcary.length];
		System.arraycopy( srcary, 0, destary, 0, srcary.length );
		newc[i] = destary;
	    } else if (contents[i] instanceof TypeName[]) {
		TypeName[] srcary = (TypeName[]) contents[i];
		TypeName[] destary = new TypeName[srcary.length];
		for (int j = 0; j < srcary.length; ++j) {
		    destary[j] = (TypeName) srcary[j].makeRecursiveCopy();
		}
		newc[i] = destary;
	    } else if (contents[i] instanceof VariableDeclarator []) {
		VariableDeclarator[] srcary = (VariableDeclarator[]) contents[i];
		VariableDeclarator[] destary =
		    new VariableDeclarator[srcary.length];
		for (int j = 0; j < srcary.length; ++j) {
		    //VariableDe
		    destary[j] = (VariableDeclarator)srcary[j].makeRecursiveCopy();
		}
		newc[i] = destary;
	    } else if (contents[i] instanceof Object[]) {
		System.err.println( "makeRecursiveCopy() not supported in " +
				     getClass() );
		newc[i] = contents[i];
	    } else {
	        newc[i] = contents[i];
	    }
	}

	result.set( newc );

	return result;
    }

    /**
     * Tests if this nonleaf-node's value equals to the specified
     * ptree-node's.
     *
     * @return  true if two values are same.
     */
    public boolean equals( ParseTree p ) {
        if(p == null)  return false;
        if(eq( this, p ))  return true;
        if(this.getClass() != p.getClass())  return false;
	
        NonLeaf nlp = (NonLeaf) p;
        System.err.println( "equals() not supported in " +
			    getClass().getName() );
		return childrenAreEqual(nlp);
	}

	/**
	 * Returns whether all of this object's children are equal to the
	 * corresponding child of <code>other</code>
	 */
	protected boolean childrenAreEqual(NonLeaf p) {
		int len = contents.length;
		if (len != p.contents.length) {
			return false;
		}
		for (int i = 0; i < len; i++) {
            final Object thatChild = p.contents[i],
                    thisChild = contents[i];
            if (thisChild == null) {
				if (thatChild != null) {
					return false;
				}
			} else if (thisChild instanceof ParseTree &&
                    thatChild instanceof ParseTree) {
                if (!((ParseTree) thisChild).equals((ParseTree) thatChild)) {
                    return false;
                }
            } else {
                if (!thisChild.equals(thatChild)) {
				    return false;
                }
			}
		}
		return true;
	}

	/**
     * Makes this ptree a list presenting for
     * [ p ]
     *
     * @param  ptrees  list's element
     */
    protected void set( Object[] ptrees )  {
	contents = ptrees;
	for (int i = 0; i < contents.length; ++i) {
	    if (contents[i] instanceof ParseTreeObject) {
		((ParseTreeObject) contents[i]).setParent( this );
	    }
	}
    }
  
    /**
     * Makes this ptree a list presenting for
     * [ p ]
     *
     * @param  p  list's element
     */
    protected void set( Object p ) {
	set( new Object[]{ p } );
    }
  
    /**
     * Makes this ptree a list presenting for
     * [ p0 p1 ]
     *
     * @param  p0  list's element
     * @param  p1  list's element
     */
    protected void set( Object p0, Object p1 ) {
	set( new Object[]{ p0, p1 } );
    }
  
    /**
     * Makes this ptree a list presenting for
     * [ p0 p1 p2 ]
     *
     * @param  p0  list's element
     * @param  p1  list's element
     * @param  p2  list's element
     */
    protected void set( Object p0, Object p1, Object p2 ) {
	set( new Object[]{ p0, p1, p2 } );
    }
  
    /**
     * Makes this ptree a list presenting for
     * [ p0 p1 p2 p3 ]
     *
     * @param p0 list's element
     * @param p1 list's element
     * @param p2 list's element
     * @param p3 list's element
     */
    protected void set( Object p0, Object p1, Object p2, Object p3 ) {
	set( new Object[]{ p0, p1, p2, p3 } );
    }
  
    /**
     * Makes this ptree a list presenting for
     * [ p0 p1 p2 p3 p4 ]
     *
     * @param p0 list's element
     * @param p1 list's element
     * @param p2 list's element
     * @param p3 list's element
     * @param p4 list's element
     */
    protected void set( Object p0, Object p1, Object p2, Object p3,
  		      Object p4 )
    {
	set( new Object[]{ p0, p1, p2, p3, p4 } );
    }
  
    /**
     * Makes this ptree a list presenting for
     * [ p0 p1 p2 p3 p4 p5 ]
     *
     * @param p0 list's element
     * @param p1 list's element
     * @param p2 list's element
     * @param p3 list's element
     * @param p4 list's element
     */
    protected void set( Object p0, Object p1, Object p2, Object p3,
  		      Object p4, Object p5 )
    {
	set( new Object[]{ p0, p1, p2, p3, p4, p5 } );
    }
  
    /**
     * Makes this ptree a list presenting for
     * [ p0 p1 p2 p3 p4 p5 p6 ]
     *
     * @param p0 list's element
     * @param p1 list's element
     * @param p2 list's element
     * @param p3 list's element
     * @param p4 list's element
     * @param p5 list's element
     * @param p6 list's element
     */
    protected void set( Object p0, Object p1, Object p2, Object p3,
  		      Object p4, Object p5, Object p6 )
    {
	set( new Object[]{ p0, p1, p2, p3, p4, p5, p6 } );
    }
  
    /**
     * Makes this ptree a list presenting for
     * [ p0 p1 p2 p3 p4 p5 p6 p7 ]
     *
     * @param p0 list's element
     * @param p1 list's element
     * @param p2 list's element
     * @param p3 list's element
     * @param p4 list's element
     * @param p5 list's element
     * @param p6 list's element
     * @param p7 list's element
     */
    protected void set( Object p0, Object p1, Object p2, Object p3,
  		      Object p4, Object p5, Object p6, Object p7 )
    {
	set( new Object[]{ p0, p1, p2, p3, p4, p5, p6, p7 } );
    }

    /**
     * Writes the code this parse-tree presents for.
     *
     * @deprecated
     */
    public void writeCode() {
        writeDebugL();
    
        if (contents.length == 0) {
	    System.err.println( "writeCode() error : no contents." );
        } else {
	    if (contents[0] instanceof ParseTree) {
		((ParseTree) contents[0]).writeCode();
	    } else {
		out.print( contents[0].toString() );
	    }
        }
        for( int i = 1; i < contents.length; i++ ){
	    out.print( " " );
	    if (contents[i] instanceof ParseTree) {
		((ParseTree) contents[i]).writeCode();
	    } else {
		out.print( contents[i].toString() );
	    }
        }
    
        writeDebugR();
    }
  
    /**
     * Returns the specified element at the specified point of this
     * nonleaf-node.
     *
     * @param  i  index
     * @return  the ptree object at the specified point
     */
    protected Object elementAt( int i ) {
      Object ret = null;
  
      try{
        ret = contents[i];
      }catch( ArrayIndexOutOfBoundsException ex ){
        System.out.println( ex );
        //throw new NonLeafException( ex.toString() );
      }
  
      return ret;
    }
  
    /**
     * Sets the specified element at the specified point of this
     * nonleaf-node.
     *
     * @param  p  ptree object to set
     * @param  i  index
     */
    protected void setElementAt( Object p, int i ) {
	try{
	    contents[i] = p;
	    if (contents[i] instanceof ParseTreeObject) {
		((ParseTreeObject) contents[i]).setParent( this );
	    }
	}catch( ArrayIndexOutOfBoundsException ex ){
	    System.out.println( ex );
	    //throw new NonLeafException( ex.toString() );
	}
    }

    /**
     * Gets the contents of this nonleaf-node.
     *
     * @return  contents
     */
    public Object[] getContents() {
	/***********/
	return contents;
    }

    /**
     * getLength() returns the length of this nonleaf-node
     *
     * @return the length of the list
     */
    protected int getLength() {
        return contents.length;
    }
  
    /**
     * Sets the comment of javadoc format which explains this declaration.
     *
     * @param  comment  the Comment object to set
     */
    public void setComment( String comment ) {
        this.comment = comment;
    }
  
    /**
     * Obtains the comment of javadoc format which explains this declaration.
     *
     * @return  the string of the comment
     */
    public String getComment() {
        return this.comment;
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
	for (int i = 0; i < contents.length; ++i) {
	    if (contents[i] instanceof ParseTree) {
		ParseTree ptree = (ParseTree) contents[i];
		ptree.accept( visitor );
	    }
	}
    }

    // default implementation of Expression -- derived classes, please do
    // better
    public OJClass getRowType( Environment env )
	throws Exception
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
