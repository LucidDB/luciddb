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
package org.eigenbase.util;

import java.io.*;

import java.util.*;

import org.eigenbase.util14.*;


/**
 * <code>EnumeratedValues</code> is a helper class for declaring a set of
 * symbolic constants which have names, ordinals, and possibly descriptions. The
 * ordinals do not have to be contiguous.
 *
 * <p>Typically, for a particular set of constants, you derive a class from this
 * interface, and declare the constants as <code>public static final</code>
 * members. Give it a private constructor, and a <code>public static final <i>
 * ClassName</i> instance</code> member to hold the singleton instance.</p>
 */
public class EnumeratedValues
    extends Enum14
{

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new empty, mutable enumeration.
     */
    public EnumeratedValues()
    {
    }

    /**
     * Creates an enumeration, with an array of values, and freezes it.
     */
    public EnumeratedValues(Value [] values)
    {
        super(values);
    }

    /**
     * Creates an enumeration, initialize it with an array of strings, and
     * freezes it.
     */
    public EnumeratedValues(String [] names)
    {
        super(names);
    }

    /**
     * Create an enumeration, initializes it with arrays of code/name pairs, and
     * freezes it.
     */
    public EnumeratedValues(
        String [] names,
        int [] codes)
    {
        super(names, codes);
    }

    /**
     * Create an enumeration, initializes it with arrays of code/name pairs, and
     * freezes it.
     */
    public EnumeratedValues(
        String [] names,
        int [] codes,
        String [] descriptions)
    {
        super(names, codes, descriptions);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a mutable enumeration from an existing enumeration, which may
     * already be immutable.
     */
    public EnumeratedValues getMutableClone()
    {
        return (EnumeratedValues) clone();
    }
}

// End EnumeratedValues.java
