/*
 * AbstractSyntaxRule.java
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


import openjava.ptree.ParseTree;


/**
 * The interface <code>AbstractSyntaxRule</code> represents a syntax rule.
 * <p>
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public abstract class AbstractSyntaxRule implements SyntaxRule
{
    /**
     * Consumes tokens from the given token source following
     * the rule.  To be overridden.
     *
     * @param  token_src token source to consume.
     * @return a parse tree object consumed by following this rule.
     */
    public abstract ParseTree consume( TokenSource token_src )
	throws SyntaxException;

    /**
     * Tests if the given token source follows this rule.
     *
     * @param  token_src token source to consume.
     * @return true if the given token source can be consumed safely.
     */
    public final boolean lookahead( TokenSource token_src ) {
	try {
	    RestorableTokenSource dummy
		= new RestorableTokenSource( token_src );
	    consume( dummy );
	    return true;
	} catch (SyntaxException e) {
	    setSyntaxException( e );
	    return false;
	}
    }

    private SyntaxException lastException = null;

    /**
     * Obtains the syntax exception at the last lookahead.
     * through the method <tt>lookahead(TokenSource)</tt>.
     *
     * @return the syntax exception.
     */
    public final SyntaxException getSyntaxException() {
	return lastException;
    }

    /**
     * Sets the last syntax exception in consuming token source
     * through the method <tt>consume(TokenSource)</tt>.
     *
     * @return the syntax exception.
     */
    private final void setSyntaxException( SyntaxException e ) {
	lastException = e;
    }

}

