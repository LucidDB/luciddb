/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
// jhyde, May 3, 2002
*/

package openjava.ptree.util;

import openjava.ptree.ParseTree;

/**
 * A <code>ParseTreeAction</code> is called when a {@link ParseTreePattern}
 * finds a match. It yields a {@link ParseTree} by substituting the matching
 * tokens.
 *
 * @author jhyde
 * @since May 3, 2002
 * @version $Id$
 **/
public interface ParseTreeAction {
	void onMatch(ParseTree[] tokens);
}

// End ParseTreeAction.java
