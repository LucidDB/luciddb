/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2003 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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

#ifndef Fennel_StoredTypeDescriptor_Included
#define Fennel_StoredTypeDescriptor_Included

FENNEL_BEGIN_NAMESPACE

class DataVisitor;

/**
 * StoredTypeDescriptor is an abstract class defining the datatypes which
 * characterize values stored in tuples, as described in
 * <a href="structTupleDesign.html#StoredTypeDescriptor">the design docs</a>.
 */
class FENNEL_TUPLE_EXPORT StoredTypeDescriptor
{
public:
    /**
     * Each type must have a unique positive integer ordinal associated with
     * it.  This is used to reconstruct a StoredTypeDescriptor object from a
     * stored attribute definition.
     */
    typedef uint Ordinal;

    virtual ~StoredTypeDescriptor();

    /**
     * @return the ordinal representing this type.
     */
    virtual Ordinal getOrdinal() const = 0;

    /**
     * @return number of bits in marshalled representation, or 0 for a non-bit
     * type; currently only 0 or 1 is supported
     */
    virtual uint getBitCount() const = 0;

    /**
     * @return the width in bytes for a fixed-width non-bit type which admits no
     * per-attribute precision, or 0 for types with per-attribute precision;
     * for bit types, this yields the size of the unmarshalled representation
     */
    virtual uint getFixedByteCount() const = 0;

    /**
     * Gets the number of bytes required to store the narrowest value with this
     * type, given a particular max byte count.  For a fixed-width
     * type, the return value is the same as the input.
     *
     * @param cbMaxWidth maximum width for which to compute the minimum
     *
     * @return number of bytes
     */
    virtual uint getMinByteCount(uint cbMaxWidth) const = 0;

    /**
     * Gets the alignment size in bytes required for values of this type, given
     * a particular max byte count.  This must be 1, 2, 4, or 8, and may not be
     * greater than 2 for variable-width datatypes.  For fixed-width datatypes,
     * the width must be a multiple of the alignment size.
     *
     * @param cbWidth width for which to compute the alignment
     *
     * @return number of bytes
     */
    virtual uint getAlignmentByteCount(uint cbWidth) const = 0;

    /**
     * Visits a value of this type.
     *
     * @param dataVisitor the DataVisitor which should be called with the
     * interpreted value
     *
     * @param pData the address of the data value
     *
     * @param cbData the number of bytes of data
     */
    virtual void visitValue(
        DataVisitor &dataVisitor,
        void const *pData,
        TupleStorageByteLength cbData) const = 0;

    /**
     * Compares two values of this type.
     *
     * @param pData1 the address of the first data value
     *
     * @param cbData1 the width of the first data value in bytes
     *
     * @param pData2 the address of the second data value
     *
     * @param cbData2 the width of the second data value in bytes
     *
     * @return negative if the first data value is less than the second;
     * positive if greater; zero if equal
     */
    virtual int compareValues(
        void const *pData1,
        TupleStorageByteLength cbData1,
        void const *pData2,
        TupleStorageByteLength cbData2) const = 0;
};

FENNEL_END_NAMESPACE

#endif

// End StoredTypeDescriptor.h
