/*
 * IterationRule.java
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


import openjava.ptree.*;


/**
 * The class <code>IterationRule</code> represents iterative syntax
 * rule.
 * <p>
 * Suppose there's a syntax rule A.  This class can represents
 * the syntax ( A )* or ( A )+
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class IterationRule extends AbstractSyntaxRule
{
    private SyntaxRule elementRule;
    private boolean allowsEmpty;

    /**
     * Allocates a new rule representing iterations of a given rule.
     *
     * @param elementRule  a rule to iterate
     * @param allowsEmpty  a flag to allow 0 iteration if it is true.
     */
    public IterationRule( SyntaxRule elementRule, boolean allowsEmpty ) {
	this.elementRule = elementRule;
	this.allowsEmpty = allowsEmpty;
    }

    /**
     * Allocates a new rule representing iterations of a given rule
     * not allowing 0 iteration.
     *
     * @param elementRule  a rule to iterate
     */
    public IterationRule( SyntaxRule elementRule ) {
	this( elementRule, false );
    }

    /**
     * Consumes token source.
     *
     * @param token_src  token source.
     * @return  null if this fails to consume a syntax tree represented
     * by this object.  Otherwise it returns <code>ObjectList</code> object.
     */
    public ParseTree consume( TokenSource token_src )
	throws SyntaxException
    {
	ObjectList result = new ObjectList();
	if (! allowsEmpty) {
	    result.add( elementRule.consume( token_src ) );
	}
	while (elementRule.lookahead( token_src )) {
	    ParseTree elem = elementRule.consume( token_src );
	    result.add( elem );
	}
	return result;
    }

}
