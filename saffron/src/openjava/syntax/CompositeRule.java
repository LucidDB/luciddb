/*
 * CompositeRule.java
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
 * The class <code>CompositeRule</code> represents iterative syntax
 * rule.
 * <p>
 * Suppose there's a syntax rules; A, B, C.  This class can represents
 * the syntax ( A B C ).
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class CompositeRule extends AbstractSyntaxRule
{
    private SyntaxRule[] elementRules;

    /**
     * Allocates a new rule representing a composition of given rules.
     *
     * @param elementRules  an array of rules
     */
    public CompositeRule( SyntaxRule[] elementRules ) {
	this.elementRules = elementRules;
    }

    public CompositeRule( SyntaxRule e1, SyntaxRule e2 ) {
	this( new SyntaxRule[] { e1, e2 } );
    }

    public CompositeRule( SyntaxRule e1, SyntaxRule e2, SyntaxRule e3 ) {
	this( new SyntaxRule[] { e1, e2, e3 } );
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
	for (int i = 0; i < elementRules.length; ++i) {
	    ParseTree elem = elementRules[i].consume( token_src );
	    result.add( elem );
	}
	return result;
    }

}
