/*
 * $Id$
 *
 * comments here.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1998 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.syntax;


import openjava.mop.Environment;
import openjava.tools.parser.Token;


/**
 * A stream of {@link Token}s.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see Token
 */
public interface TokenSource
{
    public Environment getEnvironment();
    public Token getNextToken();
    public Token getToken( int i );

}
