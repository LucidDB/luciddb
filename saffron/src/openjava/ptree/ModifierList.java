/*
 * ModifierList.java 1.0
 *  
 * @see openjava.ptree.ParseTree
 * @version last updated: 06/11/97
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import java.lang.reflect.Modifier;
import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;


/**
 * The ModifierList class presents for the node of parse tree
 * of qualified name.
 *
 */
public class ModifierList extends List
{

    /*
     * Access modifier flag constants from <em>The Java Virtual
     * Machine Specification</em>, Table 4.1.
     */
    public static final int PUBLIC	 =	Modifier.PUBLIC;
    public static final int PROTECTED	 =	Modifier.PROTECTED;
    public static final int PRIVATE	 =	Modifier.PRIVATE;
    public static final int STATIC	 =	Modifier.STATIC;
    public static final int FINAL	 =	Modifier.FINAL;
    public static final int SYNCHRONIZED =	Modifier.SYNCHRONIZED;
    public static final int VOLATILE	 =	Modifier.VOLATILE;
    public static final int TRANSIENT	 =	Modifier.TRANSIENT;
    public static final int NATIVE	 =	Modifier.NATIVE;
//    public static final int INTERFACE	 =	Modifier.INTERFACE;
    public static final int ABSTRACT	 =	Modifier.ABSTRACT;
//    public static final int STRICT	 =	Modifier.STRICT;
//    public static final int EXPLICIT	 =	Modifier.EXPLICIT;

    public static final int EMPTY	 =	0;

    private int mod = 0;

    public ModifierList() {
	super( " " );
    }

    public ModifierList( String e0 ) {
	super( " ", e0 );
    }

    public ModifierList( int mod ) {
	this();
	this.mod = mod;
    }

    public void writeCode() {
	writeDebugL();
	
	out.print( toString() );

	writeDebugR();
    }

    public String toString() {
	return toString( this.getRegular() );
    }
    
    /**
     * Return a string describing the access modifier flags in
     * the specified modifier. For example:
     * <blockquote><pre>
     *    public final synchronized
     *    private transient volatile
     * </pre></blockquote>
     * The modifier names are return in canonical order, as
     * specified by <em>The Java Language Specification</em>.
     */
    public static String toString( int mod ) {
        StringBuffer sb = new StringBuffer();
        int len;

        if ((mod & PUBLIC) != 0)        sb.append( "public " );
        if ((mod & PRIVATE) != 0)       sb.append( "private " );
        if ((mod & PROTECTED) != 0)     sb.append( "protected " );

        /* Canonical order */
        if ((mod & ABSTRACT) != 0)      sb.append( "abstract " );
        if ((mod & STATIC) != 0)        sb.append( "static " );
        if ((mod & FINAL) != 0)         sb.append( "final " );
        if ((mod & TRANSIENT) != 0)     sb.append( "transient " );
        if ((mod & VOLATILE) != 0)      sb.append( "volatile " );
        if ((mod & NATIVE) != 0)        sb.append( "native " );
        if ((mod & SYNCHRONIZED) != 0)  sb.append( "synchronized " );

//        if ((mod & INTERFACE) != 0)     sb.append( "interface " );

//        if ((mod & STRICT) != 0)        sb.append( "strict " );
//        if ((mod & EXPLICIT) != 0)      sb.append( "explicit " );

        if ((len = sb.length()) > 0)    /* trim trailing space */
            return sb.toString().substring( 0, len-1 );
        return "";
    }

    public boolean isEmpty() {
	return (super.isEmpty() && getRegular() == EMPTY);
    }

    public boolean isEmptyAsRegular() {
	return (getRegular() == EMPTY);
    }

    public boolean contains( String str ) {
	if (str == null)  return false;
        if (str.equals( "public " ) && contains( PUBLIC ))  return true;
        if (str.equals( "private " ) && contains( PRIVATE ))  return true;
        if (str.equals( "protected " ) && contains( PROTECTED ))  return true;
        if (str.equals( "abstract " ) && contains( ABSTRACT ))  return true;
        if (str.equals( "static " ) && contains( STATIC ))  return true;
        if (str.equals( "final " ) && contains( FINAL ))  return true;
        if (str.equals( "transient " ) && contains( TRANSIENT ))  return true;
        if (str.equals( "volatile " ) && contains( VOLATILE ))  return true;
        if (str.equals( "native " ) && contains( NATIVE ))  return true;
        if (str.equals( "synchronized " ) && contains( SYNCHRONIZED )) {
	    return true;
	}
	return super.contains( str );
    }

    public boolean contains( int mod ) {
	return ((this.mod & mod) != 0);
    }

    /**
     * Gets the specified element at
     *
     * @param  n  
     */
    public String get( int n ) {
	return (String) contents_elementAt( n );
    }

    /**
     * Sets the specified element at the specified place of the list
     *
     * @param  p  element
     * @param  n  the number where to set element
     */
    public void set( int n, String p ) {
	contents_setElementAt( p, n );
    }

    /**
     * Adds the specified element after the list
     * This causes side-effect.
     *
     * @param  p  a modifier  to be inserted into the list
     */
    public void add( int mod ) {
	this.mod |= mod;
    }

    /**
     * Adds the specified element after the list
     * This causes side-effect.
     *
     * @param  p  a modifier to be inserted into the list
     */
    public void add( String p ) {
	contents_addElement( p );
    }

    /**
     * Removes an element at the specified point from this list
     *
     * @param  n  where to remove element.
     */
    public String remove( int n ) {
	String removed = (String) contents_elementAt( n );
        contents_removeElementAt( n );
	return removed;
    }

    /**
     * Inserts the specified element into the list
     * before the specified element of the list.
     * This causes side-effect.
     *
     * @param  p  the element to be inserted into the list
     * @param  n  number of the element before which insertion ocuurs
     */
    public void insertElementAt( String p, int n ) {
	contents_insertElementAt( p, n );
    }

    /**
     * Appends a list after this list.
     *
     * @param  lst  a list to be appended
     */
    public void append( ModifierList lst ) {
	this.mod |= lst.getRegular();
	for( int i = 0; i < lst.size(); i++ ){
	    contents_addElement( lst.get( i ) );
	}
    }

    public int getRegular() {
	return this.mod;
    }

    public void accept(ParseTreeVisitor v) throws ParseTreeException {
        v.visit(this);
    }

}
