/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.io.*;


/**
 * FennelTupleAttributeDescriptor holds metadata describing a particular entry
 * in a tuple. These are contained in a FennelTupleDescriptor object to describe
 * the layout of a tuple. This class is JDK 1.4 compatible.
 *
 * @author Mike Bennett
 * @version $Id$
 */
public class FennelTupleAttributeDescriptor
    implements Serializable
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * SerialVersionUID created with JDK 1.5 serialver tool.
     */
    private static final long serialVersionUID = -4582426550989158154L;

    //~ Instance fields --------------------------------------------------------

    /**
     * the FennelStoredTypeDescriptor of this attribute.
     */
    public FennelStoredTypeDescriptor typeDescriptor;

    /**
     * is this attribute nullable?
     */
    public boolean isNullable;

    /**
     * the amount of storage, in bytes, taken by this attribute.
     */
    public int storageSize;

    //~ Constructors -----------------------------------------------------------

    /**
     * Default constructor -- shouldn't be used in normal situations.
     */
    public FennelTupleAttributeDescriptor()
    {
        isNullable = false;
        storageSize = 0;
    }

    /**
     * Normal constructor
     */
    public FennelTupleAttributeDescriptor(
        FennelStoredTypeDescriptor typeDescriptor,
        boolean isNullable,
        int storageSizeInit)
    {
        this.typeDescriptor = typeDescriptor;
        this.isNullable = isNullable;
        if (storageSizeInit > 0) {
            int fixedSize = typeDescriptor.getFixedByteCount();
            assert ((fixedSize == 0) || (fixedSize == storageSizeInit));
            storageSize = storageSizeInit;
        } else {
            storageSize = typeDescriptor.getFixedByteCount();
        }
    }
}

// End FennelTupleAttributeDescriptor.java
