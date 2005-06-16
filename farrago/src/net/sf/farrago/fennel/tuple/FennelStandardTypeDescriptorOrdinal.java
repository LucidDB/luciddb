/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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

package net.sf.farrago.fennel.tuple;

import java.util.*;

/**
 * Tracks the ordinal numbers assigned to the standard types as kept in fennel.
 * <p> 
 *
 * This module must be kept in sync with any changes to fennel's 
 * tuple/FennelStandardTypeDescriptor.h module!
 *
 * @author Mike Bennett
 * @version $Id$
 */
// NOTE: this would be *SO* much cleaner with jdk 1.5 Enum support!
public final class FennelStandardTypeDescriptorOrdinal
{
    public static final int STANDARD_TYPE_MIN       = 1;

    public static final int STANDARD_TYPE_INT_8     = 1;
    public static final int STANDARD_TYPE_UINT_8    = 2;
    public static final int STANDARD_TYPE_INT_16    = 3;
    public static final int STANDARD_TYPE_UINT_16   = 4;
    public static final int STANDARD_TYPE_INT_32    = 5;
    public static final int STANDARD_TYPE_UINT_32   = 6;
    public static final int STANDARD_TYPE_INT_64    = 7;
    public static final int STANDARD_TYPE_UINT_64   = 8;
    public static final int STANDARD_TYPE_BOOL      = 9;
    public static final int STANDARD_TYPE_REAL      = 10;
    public static final int STANDARD_TYPE_DOUBLE    = 11;
    public static final int STANDARD_TYPE_CHAR      = 12;
    public static final int STANDARD_TYPE_VARCHAR   = 13;
    public static final int STANDARD_TYPE_BINARY    = 14;
    public static final int STANDARD_TYPE_VARBINARY = 15;

    public static final int STANDARD_TYPE_END       = 16;
    public static final int EXTENSION_TYPE_MIN      = 1000;

    private FennelStandardTypeDescriptorOrdinal() {};

    // NOTE: this set of ordinal checks isn't really needed but it might
    // be used elsewhere in the system.
    // I'm keeping them for now - mbennett 11jan05
    /**
     * Indicates whether this ordinal represents a primitive type.
     */
    static public boolean isNative(int st) 
    {
        if (st < STANDARD_TYPE_DOUBLE) {
            return true;
        }
        return false;
    }

    /**
     * Indicates whether this ordinal represents a primitive non-boolean type.
     */
    static public boolean isNativeNotBool(int st)
    {
        if (st <= STANDARD_TYPE_DOUBLE && 
            st != STANDARD_TYPE_BOOL) {
            return true;
        }
        return false;
    }

    /**
     * Indicates whether this ordinal represents an integral native type.
     */
    static public boolean isIntegralNative(int st)
    {
        if (st <= STANDARD_TYPE_BOOL) {
            return true;
        }
        return false;
    }

    /**
     * Indicates whether this ordinal is an exact numeric.
     */
    static public boolean isExact(int st)
    {
        if (st <= STANDARD_TYPE_UINT_64) {
            return true;
        }
        return false;
    }
    
    /**
     * Indicates whether this ordinal is an approximate numeric.
     */
    static public boolean isApprox(int st)
    {
        if (st == STANDARD_TYPE_REAL ||
            st == STANDARD_TYPE_DOUBLE) {
            return true;
        }
        return false;
    }
    
    /**
     * Indicates whether this ordinal represents an array.
     */
    static public boolean isArray(int st)
    {
        if (st >= STANDARD_TYPE_CHAR &&
            st <= STANDARD_TYPE_VARBINARY) {
            return true;
        }
        return false;
    }

    /**
     * Indicates whether this ordinal represents a variable length array.
     */
    static public boolean isVariableLenArray(int st)
    {
        if (st == STANDARD_TYPE_VARCHAR ||
            st == STANDARD_TYPE_VARBINARY) {
            return true;
        }
        return false;
    }

    /**
     * Indicates whether this ordinal represents a fixed length array.
     */
    static public boolean isFixedLenArray(int st)
    {
        if (st == STANDARD_TYPE_CHAR ||
            st == STANDARD_TYPE_BINARY) {
            return true;
        }
        return false;
    }
    
    /**
     * Indicates whether this ordinal represents a text array.
     */
    static public boolean isTextArray(int st)
    {
        if (st == STANDARD_TYPE_CHAR ||
            st == STANDARD_TYPE_VARCHAR) {
            return true;
        }
        return false;
    }

    /**
     * Indicates whether this ordinal represent a binary array.
     */
    static public boolean isBinaryArray(int st)
    {
        if (st == STANDARD_TYPE_VARBINARY ||
            st == STANDARD_TYPE_BINARY) {
            return true;
        }
        return false;
    }
};

// End FennelStandardTypeDescriptorOrdinal.java
