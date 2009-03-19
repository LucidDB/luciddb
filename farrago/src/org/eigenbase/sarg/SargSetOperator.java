/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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
package org.eigenbase.sarg;

/**
 * SargSetOperator defines the supported set operators which can be used to
 * combine instances of {@link SargExpr}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public enum SargSetOperator
{
    /**
     * Set intersection over any number of children (no children => universal
     * set).
     */
    INTERSECTION,

    /**
     * Set union over any number of children (no children => empty set).
     */
    UNION,

    /**
     * Set complement over exactly one child.
     */
    COMPLEMENT
}

// End SargSetOperator.java
