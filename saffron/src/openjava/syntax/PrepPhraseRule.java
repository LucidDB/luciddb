/*
 * PrepPhraseRule.java
 *
 * comments here.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1999 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.syntax;


import java.lang.Object;
import openjava.tools.parser.*;
import openjava.ptree.*;


/**
 * The class <code>PrepPhraseRule</code> represents the syntax rule
 * of a prepositional phrase.
 * Suppose there's a syntax rule <code>A</code> and a given identifier
 * <code>i</code>.  This class can represent the syntax:
 * <pre>
 * PrepPhraseRule := i A
 * </pre>
 * <p>
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class PrepPhraseRule extends AbstractSyntaxRule
{

    private String prep;
    private SyntaxRule words;

    /**
     * Allocates a new rule representing the syntax of a prepositional
     * phrase consisting of a preposition and a syntax.
     */
    public PrepPhraseRule( String prep, SyntaxRule words ) {
	this.prep = prep;
	this.words = words;
    }

    public ParseTree consume( TokenSource token_src )
	throws SyntaxException
    {
	IdentifierRule ident = new IdentifierRule( prep );
	ident.consume( token_src );
	return words.consume( token_src );
    }

}
