/** This subclass of symbol represents (at least) terminal symbols returned 
 *  by the scanner and placed on the parse stack.  At present, this 
 *  class does nothing more than its super class.
 *  
 * @see java_cup.runtime.symbol
 * @version last updated: 06/11/97
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import java.io.*;
import openjava.ptree.util.*;
import openjava.mop.*;


public class ParseTreeException extends Exception
{
    private Exception ex = null;

    public ParseTreeException() {
    }

    public ParseTreeException( Exception e ) {
        super( e.getMessage() );
	this.ex = e;
    }

    public ParseTreeException( String str ) {
        super( str );
    }

    public void printStackTrace( PrintWriter o ) {
        if (ex != null) {
	    ex.printStackTrace( o );
	} else {
	    super.printStackTrace( o );
	}
    }

    public void printStackTrace( PrintStream o ) {
        if (ex != null) {
	    ex.printStackTrace( o );
	} else {
	    super.printStackTrace( o );
	}
    }
}
