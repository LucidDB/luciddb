/*
 * TokenRule.java
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


import openjava.tools.parser.Token;
import openjava.ptree.*;


/**
 * The class <code>TokenRule</code> represents the syntax
 * rule of a list separated by an separator.
 * <p>
 * Suppose there's a syntax rule A and token t.  This class can
 * represents the syntax A ( t A )*.
 * <p>
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public final class TokenRule extends AbstractSyntaxRule
{
    private int tokenID;

    /**
     * Allocates a new rule representing a specified token.
     *
     * @param separator_token  the id of a token.
     * @see openjava.syntax.TokenID
     */
    public TokenRule( int token_id ) {
	this.tokenID = token_id;
    }

    /**
     * Consumes token source.
     *
     * @param token_src  token source.
     * @return  null if this fails to consume a syntax tree represented
     * by this object.  Otherwise it returns <code>ObjectList</code> object.
     */
    public final ParseTree consume( TokenSource token_src )
	throws SyntaxException
    {
	Token t = token_src.getNextToken();
	if (t.kind != tokenID) {
	    /***** to become specifed error report */
	    throw new SyntaxException( "un expected token" );
	}
	return new Leaf( t.kind, t.image, t.beginLine, t.beginColumn );
    }

}
