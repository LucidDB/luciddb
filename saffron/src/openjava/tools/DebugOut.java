/*
 * DebugOut.java 1.0
 *
 * This class manages imported classes with statement
 * "import foo.Bar;" and "import foo.*;".
 *
 * Apr 7, 1998
 *
 * @version 1.0 last updated: Apr 7, 1998
 * @author  Michiaki Tatsubori
 */
package openjava.tools;


import java.lang.System;
import java.io.*;
import java.util.Enumeration;
import java.util.StringTokenizer;


/**
 * The DebugOut class is used to print something in debugging.
 * No instance should be allocate and this class should be used statically.
 *
 * <p>This class implements most methods of public printing methods
 * found in java.io.PrintWriter, as static methods.
 *
 * @version 	1.0a1, 98/04/10
 * @author	Michiaki Tatsubori
 * @since	JDK1.1
 * @see         openjava.ptree.NonLeaf
 */
public final class DebugOut
{
    private static int debugLevel = 0;

    public static void setDebugLevel( int level ) {
	debugLevel = level;
    }
	public static int getDebugLevel() {
	return debugLevel;
    }
    public static void setDebugOut(PrintStream ps) {
	out = ps;
    }

    /**
     * for debug
     */
    protected static PrintStream out = System.err;

    /**
     * Returns the output stream. You can use this stream to print regardless
     * of the value of <code>debugLevel</code> (perhaps based upon other, more
     * specific, properties).
     **/
    public static PrintStream getStream() {
	return out;
    }

    /** Flush the stream. */
    public static void flush() {
        if (debugLevel > 2)  out.flush();
    }

    /** Close the stream. */
    public static void close() {
        if (debugLevel > 2)  out.close();
    }

    /**
     * Flush the stream and check its error state.  Errors are cumulative;
     * once the stream encounters an error, this routine will return true on
     * all successive calls.
     *
     * @return True if the print stream has encountered an error, either on
     * the underlying output stream or during a format conversion.
     */
    public static boolean checkError() {
	return out.checkError();
    }

    /* Methods that do not terminate lines */

    /** Print a boolean. */
    public static void print( boolean b ) {
        if (debugLevel > 2)  out.print( b );
    }

    /** Print a character. */
    public static void print( char c ) {
        if (debugLevel > 2)  out.print( c );
    }

    /** Print an integer. */
    public static void print( int i ) {
        if (debugLevel > 2)  out.print( i );
    }

    /** Print a long. */
    public static void print( long l ) {
        if (debugLevel > 2)  out.print( l );
    }

    /** Print a float. */
    public static void print( float f ) {
        if (debugLevel > 2)  out.print( f );
    }

    /** Print a double. */
    public static void print( double d ) {
        if (debugLevel > 2)  out.print( d );
    }

    /** Print an array of chracters. */
    public static void print( char s[] ) {
        if (debugLevel > 2)  out.print( s );
    }

    /** Print a String. */
    public static void print( String s ) {
        if (debugLevel > 2)  out.print( s );
    }

    /** Print an object. */
    public static void print( Object obj ) {
        if (debugLevel > 2)  out.print( obj );
    }

    /* Methods that do terminate lines */

    /** Finish the line. */
    public static void println() {
        if (debugLevel > 2)  out.println();
    }

    /** Print a boolean, and then finish the line. */
    public static void println( boolean x ) {
        if (debugLevel > 2)  out.println( x );
    }

    /** Print a character, and then finish the line. */
    public static void println( char x ) {
        if (debugLevel > 2)  out.println( x );
    }

    /** Print an integer, and then finish the line. */
    public static void println( int x ) {
        if (debugLevel > 2)  out.println( x );
    }

    /** Print a long, and then finish the line. */
    public static void println( long x ) {
        if (debugLevel > 2)  out.println( x );
    }

    /** Print a float, and then finish the line. */
    public static void println( float x ) {
        if (debugLevel > 2)  out.println( x );
    }

    /** Print a double, and then finish the line. */
    public static void println( double x ) {
        if (debugLevel > 2)  out.println( x );
    }

    /** Print an array of characters, and then finish the line. */
    public static void println( char x[] ) {
        if (debugLevel > 2)  out.println( x );
    }

    /** Print a String, and then finish the line. */
    public static void println( String x ) {
        if (debugLevel > 2)  out.println( x );
    }

    /** Print an Object, and then finish the line. */
    public static void println( Object x ) {
        if (debugLevel > 2)  out.println( x );
    }

}



