/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Tech
// You must accept the terms in LICENSE.html to use this software.
// jhyde, May 3, 2002
*/

package openjava.ptree.util;

import openjava.ptree.ParseTree;

/**
 * A <code>ParseTreePattern</code> represents an expression with holes in it.
 * The {@link #match} method tests whether a given expression matches the
 * pattern.
 *
 * @author jhyde
 * @since May 3, 2002
 * @version $Id$
 **/
public interface ParseTreePattern {
	/**
	 * Calls <code>action</code> for every combination of tokens for which
	 * this pattern matches.
	 */
	void match(ParseTree ptree, ParseTreeAction action);
}

// End ParseTreePattern.java
