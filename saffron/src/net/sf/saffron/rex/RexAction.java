/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
// jhyde, May 3, 2002
*/

package net.sf.saffron.rex;

/**
 * A <code>RexAction</code> is called when a {@link RexPattern}
 * finds a match. It yields a {@link RexNode} by substituting the matching
 * tokens.
 *
 * @author jhyde
 * @since May 3, 2002
 * @version $Id$
 **/
public interface RexAction {
	void onMatch(RexNode[] tokens);
}

// End RexAction.java
