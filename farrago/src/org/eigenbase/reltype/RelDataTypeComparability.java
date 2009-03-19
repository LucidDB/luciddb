/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package org.eigenbase.reltype;

import org.eigenbase.util.*;


/**
 * RelDataTypeComparability is an enumeration of the categories of comparison
 * operators which types may support.
 *
 * <p>NOTE jvs 17-Mar-2005: the order of values of this enumeration is
 * significant (from least inclusive to most inclusive) and should not be
 * changed.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public enum RelDataTypeComparability
{
    None("No comparisons allowed"), Unordered("Only equals/not-equals allowed"),
    All("All comparisons allowed");

    RelDataTypeComparability(String description)
    {
        Util.discard(description);
    }
}

// End RelDataTypeComparability.java
