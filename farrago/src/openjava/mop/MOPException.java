/*
 * MOPException.java
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1999 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.mop;


import java.io.*;
import java.lang.Exception;


/**
 * MOPException is thrown if the requested introspection or intercession
 * cannot be performed on the class, field, method or constructor object.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class MOPException extends Exception
{
    private Exception ex = null;

    public MOPException() {
    }

    public MOPException( Exception e ) {
        super( e.getMessage() );
	this.ex = e;
    }

    public MOPException( String message ) {
	super( message );
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
