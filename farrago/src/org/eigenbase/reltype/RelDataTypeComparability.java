/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
 * RelDataTypeComparability is an enumeration of the categories of
 * comparison operators which types may support.
 *
 *<p>
 *
 * NOTE jvs 17-Mar-2005:  the ordinal values of this enumeration are
 * significant (from least inclusive to most inclusive) and should
 * not be changed.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RelDataTypeComparability extends EnumeratedValues.BasicValue
{
    public static final int None_ordinal = 0;
    public static final RelDataTypeComparability None =
        new RelDataTypeComparability(
            "NONE", None_ordinal, "No comparisons allowed");
    
    public static final int Unordered_ordinal = 1;
    public static final RelDataTypeComparability Unordered =
        new RelDataTypeComparability(
            "UNORDERED", Unordered_ordinal, "Only equals/not-equals allowed");
    
    public static final int All_ordinal = 2;
    public static final RelDataTypeComparability All =
        new RelDataTypeComparability(
            "ALL", All_ordinal, "All comparisons allowed");

    private RelDataTypeComparability(
        String name,
        int ordinal,
        String description)
    {
        super(name, ordinal, description);
    }
}

// End RelDataTypeComparability.java
