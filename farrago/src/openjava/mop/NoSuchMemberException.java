/*
 * NoSuchMemberException.java
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
 * The exception <code>NoSuchMemberException</code> is thrown
 * when the requested class object does not have the coressponding
 * member.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class NoSuchMemberException extends MOPException
{
    public NoSuchMemberException() {
	super();
    }

    public NoSuchMemberException( Exception e ) {
	super( e );
    }

    public NoSuchMemberException( String access ) {
	super( access );
    }
}
