/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_StoredTypeDescriptorFactory_Included
#define Fennel_StoredTypeDescriptorFactory_Included

FENNEL_BEGIN_NAMESPACE

class StoredTypeDescriptor;

/**
 * StoredTypeDescriptorFactory is an abstract factory defining how
 * StoredTypeDescriptors are instantiated, as described in
 * <a href="structTupleDesign.html#StoredTypeDescriptor">the design docs</a>.
 */
class StoredTypeDescriptorFactory
{
public:
    virtual ~StoredTypeDescriptorFactory();
    
    /**
     * Instantiates a StoredTypeDescriptor.
     *
     *<p>
     *
     * TODO:  extend this to cover type parameters such as precision and scale?
     *
     * @param iTypeOrdinal the ordinal for the type
     *
     * @return the corresponding data type object
     */
    virtual StoredTypeDescriptor const &newDataType(
        StoredTypeDescriptor::Ordinal iTypeOrdinal) const = 0;
};

FENNEL_END_NAMESPACE

#endif

// End StoredTypeDescriptorFactory.h
