/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
// jhyde, May 3, 2002
*/

package org.eigenbase.rex;

/**
 * A <code>RexPattern</code> represents an expression with holes in it.
 * The {@link #match} method tests whether a given expression matches the
 * pattern.
 *
 * @author jhyde
 * @since May 3, 2002
 * @version $Id$
 **/
public interface RexPattern {
	/**
	 * Calls <code>action</code> for every combination of tokens for which
	 * this pattern matches.
	 */
	void match(RexNode ptree, RexAction action);
}

// End RexPattern.java
