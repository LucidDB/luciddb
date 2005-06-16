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

import java.util.HashMap;

/**
 * Implements a singleton factory for creating StoredTypeDescriptors 
 * for the standard ordinal types.
 *
 * <p> 
 *
 * New stored (user-defined) types can be added to the standard factory 
 * using the getInstance().addType() method.
 */
// NOTE: there are some cleanup things that can be done to make this
// more generic, such as making the loadStandardTypes() static so that
// a user-defined factory can be created that doesn't need to inherit
// from this, but still allows loading the standard types
//

public class FennelStandardTypeDescriptorFactory
    implements FennelStoredTypeDescriptorFactory
{
    /**
     * collection containing all known standard types
     */
    private final HashMap types = new HashMap();

    /**
     *  singleton instance created at module load time
     */
    private static final FennelStandardTypeDescriptorFactory instance =
        new FennelStandardTypeDescriptorFactory();

    /**
     * get the singleton instance
     */
    static public FennelStoredTypeDescriptorFactory getInstance()
    { 
        return instance; 
    }

    /**
     * don't allow creation of instances outside this class
     */ 
    private FennelStandardTypeDescriptorFactory()
    {
        loadStandardTypes();
    }

    /**
     * adds a new typedescriptor for this factory
     * convenience function for private use
     */ 
    private final void addType(FennelStoredTypeDescriptor t)
    {
        addType(t.getOrdinal(), t);
    }

    /** 
     * loads the standard types
     */
    public final void loadStandardTypes()
    {
        addType(new FennelStandardTypeDescriptor.stdINT_8());
        addType(new FennelStandardTypeDescriptor.stdUINT_8());
        addType(new FennelStandardTypeDescriptor.stdINT_16());
        addType(new FennelStandardTypeDescriptor.stdUINT_16());
        addType(new FennelStandardTypeDescriptor.stdINT_32());
        addType(new FennelStandardTypeDescriptor.stdUINT_32());
        addType(new FennelStandardTypeDescriptor.stdINT_64());
        addType(new FennelStandardTypeDescriptor.stdUINT_64());
        addType(new FennelStandardTypeDescriptor.stdBOOL());
        addType(new FennelStandardTypeDescriptor.stdREAL());
        addType(new FennelStandardTypeDescriptor.stdDOUBLE());
        addType(new FennelStandardTypeDescriptor.stdCHAR());
        addType(new FennelStandardTypeDescriptor.stdVARCHAR());
        addType(new FennelStandardTypeDescriptor.stdBINARY());
        addType(new FennelStandardTypeDescriptor.stdVARBINARY());
    }
    
    /**
     * Adds a new typedescriptor for this factory
     * implements FennelStoredTypeDescriptorFactory
     *
     * @param ordinalType the new ordinal type to track
     *
     * @param storedType the new FennelStoredTypeDescriptor type
     * to return from calls to newDataType for this ordinalType 
     *
     */ 
    public void addType(int ordinalType, FennelStoredTypeDescriptor storedType)
    {
        types.put(new Integer(ordinalType), storedType);
    }

    /**
     * Instantiates a FennelStoredTypeDescriptor.
     * implements FennelStoredTypeDescriptorFactory
     *
     * @param iTypeOrdinal the ordinal for the type
     *
     * @return the corresponding data type object
     */
    public FennelStoredTypeDescriptor newDataType(int iTypeOrdinal)
    {
        return (FennelStoredTypeDescriptor) types.get(
            new Integer(iTypeOrdinal));
    }
};

// End FennelStandardTypeDescriptorFactory.java
