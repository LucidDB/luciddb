/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
package org.eigenbase.jmi;

/**
 * JmiAssocMapping enumerates the possible ways an association edge can be
 * mapped when a JMI graph is being transformed.
 *
 * @author John Sichi
 * @version $Id$
 */
public enum JmiAssocMapping
{
    /**
     * The association should be left out of the transformed graph.
     */
    REMOVAL,

    /**
     * The association edge should be preserved in the transformed graph.
     */
    COPY,

    /**
     * The association edge should be preserved in the transformed graph, but
     * its direction should be reversed.
     */
    REVERSAL,

    /**
     * The two ends of the association should be contracted into one vertex in
     * the transformed graph.
     */
    CONTRACTION,

    /**
     * The association edge should be interpreted as a hierarchical structure to
     * be superimposed on the graph, with the source as parent and the target as
     * child.
     */
    HIERARCHY
}

// End JmiAssocMapping.java
