/*
 * SyntaxRule.java
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
 * The interface <code>SyntaxRule</code> represents a syntax rule.
 * <p>
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public interface SyntaxRule extends TokenID
{
    /**
     * Consumes tokens from the given token source following
     * the rule.
     *
     * @param  token_src token source to consume.
     * @return null in case of fail to consume, otherwise a parse tree
     *         object consumed following this rule.
     * @exception SyntaxException in case to fail to consume.
     */
    public ParseTree consume( TokenSource token_src )
	throws SyntaxException;

    /**
     * Tests if the given token source follows this rule.
     *
     * @param  token_src token source to consume.
     * @return true if the given token source can be consumed safely.
     */
    public boolean lookahead( TokenSource token_src );

    /**
     * Returns the last syntax exception in consuming token source
     * through the method <tt>consume(TokenSource)</tt>.
     *
     * @return the syntax exception.
     */
    public SyntaxException getSyntaxException();
}
