/*
 * OJClassNotFoundException.java
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1999 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.mop;


/**
 * The exception <code>OJClassNotFoundException</code> is thrown
 * when an application tries to load in a class through its
 * string name using forName method in class OJClass, but no
 * definition for the class with the specifed name could be found.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.ClassNotFoundException
 */
public class OJClassNotFoundException extends MOPException
{
    public OJClassNotFoundException() {
        super();
    }

    public OJClassNotFoundException( String access ) {
        super( access );
    }
}
