/*
 * SeparatedListRule.java
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
 * The class <code>SeparatedListRule</code> represents the syntax
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
public abstract class SeparatedListRule extends AbstractSyntaxRule
{
    private SyntaxRule elementRule;
    private int separator;
    private boolean allowsEmpty;

    protected abstract void initList();
    protected abstract void addListElement( Object elem );
    protected abstract ParseTree getList();

    /**
     * Allocates a new rule representing a list of a give rule
     * separeted by a given separator.
     *
     * @param elementRule a rule of each element of the list
     * @param separator_token  the id of a token to be separator
     * @param allowEmpty a flag to allow 0 iteration if it is true.
     * @see openjava.syntax.TokenID
     */
    public SeparatedListRule( SyntaxRule elementRule, int separator_token,
			      boolean allowsEmpty )
    {
	this.elementRule = elementRule;
	this.separator = separator_token;
	this.allowsEmpty = allowsEmpty;
    }

    /**
     * Allocates a new rule representing a list of a give rule
     * separeted by a given separator.
     *
     * @param elementRule a rule of each element of the list
     * @param separator_token  the id of a token to be separator
     * @see openjava.syntax.TokenID
     */
    public SeparatedListRule( SyntaxRule elementRule, int separator_token ) {
	this( elementRule, separator_token, false );
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
	initList();
	ParseTree elem;
	if (! allowsEmpty) {
	    elem = elementRule.consume( token_src );
	    addListElement( elem );
	}
	CompositeRule spy
	    = new CompositeRule( new TokenRule( separator ), elementRule );
	while (spy.lookahead( token_src )) {
	    elem = consumeSepAndElem( token_src );
	    addListElement( elem );
        }
        return getList();
    }

    private ParseTree consumeSepAndElem( TokenSource token_src )
	throws SyntaxException
    {
	token_src.getNextToken(); /* separator */
	return elementRule.consume( token_src );
    }

}
