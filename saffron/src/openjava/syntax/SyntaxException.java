/*
 * SyntaxException.java
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


import openjava.tools.parser.ParseException;
import openjava.tools.parser.Token;


/**
 * Exception which occurs while processing syntax rules.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see SyntaxRule
 */
public class SyntaxException extends ParseException
{
    public SyntaxException( ParseException e ) {
        super( e.currentToken, e.expectedTokenSequences, e.tokenImage );
    }

    public SyntaxException( Token currentToken,
                            int[][] expectedToken,
                            String[] tokenImage ) {
        super( currentToken, expectedToken, tokenImage );
    }

    public SyntaxException() {
        super();
    }

    public SyntaxException( String message ) {
        super( message );
    }
}
