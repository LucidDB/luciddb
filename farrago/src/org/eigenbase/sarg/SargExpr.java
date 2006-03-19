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
package org.eigenbase.sarg;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

import java.util.*;

/**
 * SargExpr represents an expression defining a possibly non-contiguous
 * search subset of a scalar domain of a given datatype.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface SargExpr
{
    /**
     * Overrides the default Object.toString.  The result must be
     * safe for use in a RelNode digest.
     */
    public String toString();

    /**
     * @return datatype for coordinates of search domain
     */
    public RelDataType getDataType();

    /**
     * Resolves this expression into a fixed {@link SargIntervalSequence}.
     *
     *<p>
     *
     * TODO jvs 17-Jan-2006:  add binding for dynamic params so they
     * can be evaluated as well
     *
     * @return immutable ordered sequence of disjoint intervals
     */
    public SargIntervalSequence evaluate();

    /**
     * @return the factory which produced this expression
     */
    public SargFactory getFactory();

    /**
     * Collects all dynamic parameters referenced by this
     * expression.
     *
     * @param dynamicParams receives dynamic parameter references
     */
    public void collectDynamicParams(Set<RexDynamicParam> dynamicParams);
}

// End SargExpr.java
