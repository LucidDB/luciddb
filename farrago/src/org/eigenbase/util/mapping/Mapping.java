/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package org.eigenbase.util.mapping;

import java.util.*;


/**
 * A <dfn>Mapping</dfn> is a relationship between a source domain to target
 * domain of integers.
 *
 * <p>This interface represents the most general possible mapping. Depending on
 * the {@link MappingType} of a particular mapping, some of the operations may
 * not be applicable. If you call the method, you will receive a runtime error.
 * For instance:
 * <li>If a target has more than one source, then the method {@link
 * #getSource(int)} will throw {@link Mappings.TooManyElementsException}.
 * <li>If a source has no targets, then the method {@link #getTarget} will throw
 * {@link Mappings.NoElementException}.
 */
public interface Mapping
    extends Mappings.FunctionMapping,
        Mappings.SourceMapping,
        Mappings.TargetMapping,
        Iterable<IntPair>
{

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns an iterator over the elements in this mapping.
     *
     * <p>This method is optional; implementations may throw {@link
     * UnsupportedOperationException}.
     */
    Iterator<IntPair> iterator();

    /**
     * Returns the number of sources. Valid sources will be in the range 0 ..
     * sourceCount.
     */
    int getSourceCount();

    /**
     * Returns the number of targets. Valid targets will be in the range 0 ..
     * targetCount.
     */
    int getTargetCount();

    MappingType getMappingType();

    /**
     * Returns whether this mapping is the identity.
     */
    boolean isIdentity();
}

// End Mapping.java
