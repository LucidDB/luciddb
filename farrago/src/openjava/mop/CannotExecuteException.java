/*
 * CannotExecuteException.java
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1999 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.mop;


import java.lang.IllegalAccessException;


/**
 * CannotExecuteException is thrown if the requested introspection
 * cannot be performed on the class object, the method object, or
 * the field object,  which needs a java's Class object not available.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class CannotExecuteException extends MOPException
{
    public CannotExecuteException() {
	super();
    }

    public CannotExecuteException( String access ) {
	super( access );
    }
}
