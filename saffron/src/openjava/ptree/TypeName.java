/*
 * TypeName.java 1.0
 *
 * This interface is made to type ptree-node into the type
 * specifier in the body of class.
 *
 * Jun 20, 1997 by mich
 * Sep 29, 1997 by bv
 * Oct 11, 1997 by mich
 * Dec 27, 1998 by mich	
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Oct 11, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;
import java.util.Hashtable;


/**
 * The <code>TypeName</code> class represents a type specifier
 * node of parse tree.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 */
public class TypeName extends NonLeaf
{
    Hashtable suffixes;

    int dim;

    public TypeName( String typename, int n, Hashtable suffixes ) {
	super();
	set( typename );
	setDimension( n );
	this.suffixes = suffixes;
    }

    /**
     * Allocates a new object.
     *
     * @param typename type name
     * @param n array dimension
     */
    public TypeName( String typename, int n ) {
	this( typename, n, null );
    }

    /**
     * Allocates a new object.
     *
     * @param typename type name
     * @param n array dimension
     */
    public TypeName( String typename, Hashtable suffixes ) {
	this( typename, 0, suffixes );
    }

    /**
     * Allocates a new object.
     * No array dimension will set.
     * <br><blockquote><pre>
     *     new TypeName( typename )
     * </pre></blockquote><br>
     * equals:
     * <br><blockquote><pre>
     *     new TypeName( typename, 0 )
     * </pre></blockquote><br>
     *
     * @param typename type name
     */
    public TypeName( String typename ) {
	this( typename, 0, null );
    }

    TypeName() {
	super();
    }

    public static TypeName forOJClass( OJClass clazz ) {
        int demension = 0;
        while ( clazz.isArray() ) {
	    ++demension;
	    clazz = clazz.getComponentType();
	}
		// The 'null' type is no use for declarations, which is probably
		// what this TypeName will be used for, so implicitly convert it.
		// Although null can be held in any object type, we prefer Object.
		if (clazz == OJSystem.NULLTYPE) {
			clazz = OJSystem.OBJECT;
		}
	String name = clazz.getName().replace('$', '.');
		// Convert inner class separators into "$"s, e.g.
		// "com.package.Outer.Inner1.Inner2" becomes
		// "com.package.Outer$Inner1$Inner2".
		for (OJClass c = clazz; c.getDeclaringClass() != null;
				c = c.getDeclaringClass()) {
			int lastDot = name.lastIndexOf('.');
			assert(lastDot >= 0);
			name = name.substring(0, lastDot) + "$" + name.substring(lastDot + 1);
		}
        return new TypeName( name, demension );
    }

	public static TypeName forClass( Class clazz ) {
		return TypeName.forOJClass(OJClass.forClass(clazz));
	}

    public ParseTree makeRecursiveCopy() {
	TypeName result = (TypeName) super.makeRecursiveCopy();
	result.dim = this.dim;
	result.suffixes = this.suffixes;
	return result;
    }

    public ParseTree makeCopy() {
	TypeName result = (TypeName) super.makeCopy();
	result.dim = this.dim;
	result.suffixes = this.suffixes;
	return result;
    }
  
    public void writeCode() {
	writeDebugL();
	
	String typename = getName();
	out.print( typename.replace('$','.') );
    
	int dims = getDimension();
	out.print( stringFromDimension( dims ) );
    
	writeDebugR();
    }
  
    /**
     * Gets array dimension of declarated type
     *
     * @return array dimension
     */
    public int getDimension() {
	return dim;
    }
  
    /**
     * Sets array dimension of declarated type
     *
     * @param dim array dimension
     */
    public void setDimension( int n ) {
	dim = n;
    }

    public void addDimension( int n ) {
	this.dim += n;
    }

    public void addDimension( String dimstr ) {
	this.addDimension( dimstr.length() / 2 );
    }
  
    /**
     * Gets the type name of this type specifier.
     *
     * @return  the type name.
     */
    public String getName() {
	return (String) elementAt( 0 );
    }

    /**
     * Sets the type name of this type specifier.
     *
     * @param typename the type name to set.
     * @deprecated
     * @see openjava.ptree.TypeName#setName(String)
     */
    public void setTypeName( String typename ) {
	setElementAt( typename, 0 );
    }

    /**
     * Sets the type name except array dimension of this type specifier.
     *
     * @param name the type name to set.
     */
    public void setName( String name ) {
	setElementAt( name, 0 );
    }

    public static String stringFromDimension( int dimension ) {
	StringBuffer buf = new StringBuffer();
	for (int i = 0; i < dimension; ++i) {
	    buf.append( "[]" );
	}
	return buf.toString();
    }

    public static int toDimension( String typename ) {
	int result = 0;
	for (int i = typename.length() - 1; 0 < i; i -= 2) {
	    if (typename.lastIndexOf( ']', i) != i
		|| typename.lastIndexOf( '[', i) != i - 1) {
		return result;
	    }
	    result++;
	}
	return result;
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}
