/*
 * AmbiguousClassesException.java
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1999 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.mop;


import java.lang.Exception;


/**
 * The exception <code>AmbiguousClassesException</code> is thrown if the
 * additional <code>OJClass</code> object has the same name with another
 * <code>OJClass</code> object's.
 * <p>
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class AmbiguousClassesException extends MOPException {
    public AmbiguousClassesException( String access ) {
	super( access );
    }
}
