/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

package org.eigenbase.rel.convert;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.CallingConvention;


/**
 * A <code>ConverterFactory</code> is ...
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Jun 18, 2003
 */
public interface ConverterFactory
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Returns the calling convention this converter converts to.
     */
    CallingConvention getConvention();

    /**
     * Returns the calling convention this converter converts from.
     */
    CallingConvention getInConvention();

    /**
     * Converts a relational expression, which must have
     * {@link #getInConvention} calling convention, to a relational expression
     * of {@link #getConvention} calling convention.
     */
    ConverterRel convert(RelNode rel);
}


// End ConverterFactory.java
