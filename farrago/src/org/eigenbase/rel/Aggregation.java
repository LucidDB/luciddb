/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

package org.eigenbase.rel;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;


/**
 * An <code>Aggregation</code> aggregates a set of values into one value.
 *
 * <p>
 * It is used, via a {@link org.eigenbase.rel.AggregateRel.Call}, in an {@link
 * AggregateRel} relational operator.
 * </p>
 *
 * <p> To help you understand the terminology, here are some analogies: an
 * {@link Aggregation} is analogous to a {@link openjava.mop.OJMethod}, whereas
 * a {@link org.eigenbase.rel.AggregateRel.Call} is analogous to a {@link
 * openjava.ptree.MethodCall}. {@link net.sf.saffron.core.AggregationExtender}
 * has no direct analog in Java: it is more like a function object in JScript.
 * </p>
 *
 * <p> For user-defined aggregates, use you should generally use {@link
 * org.eigenbase.relopt.AggregationExtender}; writing a new
 * <code>Aggregation</code> is a complicated task, akin to writing a new
 * relational operator ({@link RelNode}).  </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @see net.sf.saffron.core.AggregationExtender
 * @since 26 January, 2001
 */
public interface Aggregation
{
    //~ Methods ---------------------------------------------------------------

    RelDataType [] getParameterTypes(RelDataTypeFactory typeFactory);

    RelDataType getReturnType(RelDataTypeFactory typeFactory);

    String getName();
}


// End Aggregation.java
