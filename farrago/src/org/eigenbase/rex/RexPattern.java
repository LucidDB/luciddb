/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
public interface RexPattern
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Calls <code>action</code> for every combination of tokens for which
     * this pattern matches.
     */
    void match(
        RexNode ptree,
        RexAction action);
}


// End RexPattern.java
