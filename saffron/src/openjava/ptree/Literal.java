/*
 * Literal.java 1.0
 *
 * Jun 20, 1997
 * Sep 29, 1997
 * Oct 10, 1997
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Oct 10, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;
import java.sql.Time;

import org.eigenbase.util.Util;



/**
 * The <code>Literal class</code> represents
 * a literal.
 *
 * @see openjava.ptree.Leaf
 * @see openjava.ptree.Expression
 */
public class Literal extends Leaf
    implements Expression
{
    /**
     * @see openjava.ptree.Literal#getLiteralType()
     */
    public static final int BOOLEAN	= 0;
    public static final int INTEGER	= 1;
    public static final int LONG	= 2;
    public static final int FLOAT	= 3;
    public static final int DOUBLE	= 4;
    public static final int CHARACTER	= 5;
    public static final int STRING	= 6;
    public static final int NULL	= 7;

    protected int id = -1;

    private static Literal constantTrue = null;
    private static Literal constantFalse = null;
    private static Literal constantNull = null;
    private static Literal constantEmptyString = null;
    private static Literal constantZero = null;
    private static Literal constantOne = null;

    /**
     * Allocates a new object.
     * If you want to make <code>String</code> literal like
     * <code>"test"</code>,
     * call this constructor in the form :
     * <br><blockquote><pre>
     *     new Literal( Literal.STRING, "\"test\"" )
     * </pre></blockquote><br>
     * or use makeLiteral() static method. 
     *
     * @param  id  the id number of the literal.
     * @param  str  the literal as a String.
     * @see openjava.ptree.Literal#makeLiteral(String)
     */  
    public Literal( int id, String str ) {
	super( str );
	this.id = id;
    }

    /**
     * Makes a new object of <code>Literal</code> class
     * from the string.
     *
     * @param  str  the string.
     */
    public static Literal makeLiteral( String str ) {
	if ("".equals( str )) {
	    return constantEmptyString();
	} else {
	    return new Literal( Literal.STRING, doubleQuoteString(str) );
	}
    }

	private static String doubleQuoteString(String s) {
		String s1 = Util.replace(s, "\\", "\\\\"),
				s2 = Util.replace(s1, "\"", "\\\""),
				s3 = Util.replace(s2, "\n\r", "\\n"),
				s4 = Util.replace(s3, "\n", "\\n"),
				s5 = Util.replace(s4, "\r", "\\r");
		return "\"" + s5 + "\"";
	}

	/**
     * Makes a new object of <code>Literal</code> class
     * from the boolean.
     *
     * @param  b  the boolean.
     */
    public static Literal makeLiteral( boolean b ) {
	return (b ? constantTrue() : constantFalse());
    }
    public static Literal makeLiteral( Boolean b ) {
	return makeLiteral( b.booleanValue() );
    }

    /**
     * Makes a new object of <code>Literal</code> class
     * from the character.
     *
     * @param  c  the character.
     */
    public static Literal makeLiteral( char c ) {
	return new Literal( Literal.CHARACTER, singleQuoteChar(c)  );
    }

	private static String singleQuoteChar(char c) {
		if (c < 32) {
			return "'\\" + ((int) c) + "'";
		} else if (c == '\'') {
			return "'\\''";
		} else {
			return "'" + c  + "'";
		}
	}

	public static Literal makeLiteral( Character c ) {
	return makeLiteral( c.charValue() );
    }

    /**
     * Makes a new object of <code>Literal</code> class
     * from the number.
     *
     * @param  num  the number.
     */
    public static Literal makeLiteral( int num ) {
	if (num == 0) {
	    return constantZero();
	} else if (num == 1) {
	    return constantOne();
	}
	return new Literal( Literal.INTEGER, String.valueOf( num ) );
    }
    public static Literal makeLiteral( Integer n ) {
	return makeLiteral( n.intValue() );
    }

    /**
     * Makes a new object of <code>Literal</code> class
     * from the number.
     *
     * @param  num  the number.
     */
    public static Literal makeLiteral( long num ) {
	return new Literal( Literal.LONG, String.valueOf( num ) + "l" );
    }
    public static Literal makeLiteral( Long n ) {
	return makeLiteral( n.longValue() );
    }

    /**
     * Makes a new object of <code>Literal</code> class
     * from the number.
     *
     * @param  num  the number.
     */
    public static Literal makeLiteral( float num ) {
	return new Literal( Literal.FLOAT, String.valueOf( num ) + "f" );
    }
    public static Literal makeLiteral( Float f ) {
	return makeLiteral( f.floatValue() );
    }

    /**
     * Makes a new object of <code>Literal</code> class
     * from the number.
     *
     * @param  num  the number.
     */
    public static Literal makeLiteral( double num ) {
	return new Literal( Literal.DOUBLE, String.valueOf( num ) + "d" );
    }
    public static Literal makeLiteral( Double d ) {
	return makeLiteral( d.doubleValue() );
    }

    public int getLiteralType() {
	return this.id;
    }

    public static Literal constantTrue() {
	if (constantTrue == null) {
	    constantTrue = new Literal( Literal.BOOLEAN, "true" );
	}
	return constantTrue;
    }
    public static Literal constantFalse() {
	if (constantFalse == null) {
	    constantFalse = new Literal( Literal.BOOLEAN, "false" );
	}
	return constantFalse;
    }
    public static Literal constantNull() {
	if (constantNull == null) {
	    constantNull = new Literal( Literal.NULL, "null" );
	}
	return constantNull;
    }
    public static Literal constantEmptyString() {
	if (constantEmptyString == null) {
	    constantEmptyString = new Literal( Literal.STRING, "\"\"" );
	}
	return constantEmptyString;
    }
    public static Literal constantZero() {
	if (constantZero == null) {
	    constantZero = new Literal( Literal.INTEGER, "0" );
	}
	return constantZero;
    }
    public static Literal constantOne() {
	if (constantOne == null) {
	    constantOne = new Literal( Literal.INTEGER, "1" );
	}
	return constantOne;
    }

    public OJClass getType( Environment env )
        throws Exception
    {
	switch (getLiteralType()) {
	case BOOLEAN :
	    return OJClass.forClass( boolean . class );
	case INTEGER :
	    return OJClass.forClass( int . class );
	case LONG :
	    return OJClass.forClass( long . class );
	case FLOAT :
	    return OJClass.forClass( float . class );
	case DOUBLE :
	    return OJClass.forClass( double . class );
	case CHARACTER :
	    return OJClass.forClass( char . class );
	case STRING :
	    return OJClass.forClass( String . class );
	case NULL :
	    return OJClass.forName( OJSystem.NULLTYPE_NAME );
	}
	System.err.println( "unknown literal : " + toString() );
	return null;
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

    /**
     * Given a double-quoted string, returns the string without double-quotes.
     **/
    public static String stripString(String s)
    {
	return s.substring(1, s.length() - 1);
    }
}
