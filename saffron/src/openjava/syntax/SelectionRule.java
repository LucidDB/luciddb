/*
 * SelectionRule.java
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


import java.lang.Object;
import openjava.tools.parser.*;
import openjava.ptree.*;


/**
 * The class <code>SelectionRule</code> represents selective syntax
 * rule.
 * <p>
 * Suppose there're several syntax rules; A, B, C.  This class
 * can represents the syntax ( A | B | C ).
 * If both A and B are adaptable to token source, A is choosed since
 * A is specified at lefter part than B's part.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class SelectionRule extends AbstractSyntaxRule
{
    protected SyntaxRule[] elementRules;

    /**
     * Allocates a new rule representing a selection of given rules.
     *
     * @param elementRules  an array of rules
     */
    public SelectionRule( SyntaxRule[] elementRules ) {
	this.elementRules = elementRules;
    }

    public SelectionRule( SyntaxRule e1, SyntaxRule e2 ) {
	this( new SyntaxRule[] { e1, e2 } );
    }

    public SelectionRule( SyntaxRule e1, SyntaxRule e2, SyntaxRule e3 ) {
	this( new SyntaxRule[] { e1, e2, e3 } );
    }

    public ParseTree consume( TokenSource token_src )
	throws SyntaxException
    {
	for (int i = 0; i < elementRules.length; ++i) {
	    if (elementRules[i].lookahead( token_src )) {
		ParseTree result = elementRules[i].consume( token_src );
		return result;
	    }
	}
	/***** to become specifed error report */
	throw new SyntaxException( "neither of selection" );
    }

}
