/*
 * CannotInspectException.java
 *
 * comments here.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1999 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.mop;


import java.lang.Object;


/**
 * The exception <code>CannotInspectException</code> is thrown if the
 * requested introspection cannot be performed on the class object,
 * the method object, or the field object.
 * <p>
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class CannotInspectException extends MOPException
{

    public CannotInspectException() {
	super();
    }

    public CannotInspectException( String message ) {
	super( message );
    }

}
