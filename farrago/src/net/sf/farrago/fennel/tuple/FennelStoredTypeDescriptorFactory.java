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

/**
 * FennelStoredTypeDescriptorFactory is an abstract interface for creating
 * StoredTypeDescriptors as described in the fennel tuple 
 * <a href="http://fennel.sourceforge.net/doxygen/html/structTupleDesign.html">
 * design document</a>
 */
public interface FennelStoredTypeDescriptorFactory
{
    /**
     * Adds a new typedescriptor to this factory.
     *
     * @param ordinalId the new ordinal to track
     *
     * @param storedType  the new typeDescriptor to return from calls to
     * newDataType for this ordinalType 
     *
     */ 
    public void addType(int ordinalId, FennelStoredTypeDescriptor storedType);

    /**
     * Instantiates a FennelStoredTypeDescriptor.
     *
     *<p>
     *
     * @param ordinalId the ordinal for the type
     *
     * @return the corresponding data type object
     */
    public FennelStoredTypeDescriptor newDataType(int ordinalId);
};

// End FennelStoredTypeDescriptorFactory.java
