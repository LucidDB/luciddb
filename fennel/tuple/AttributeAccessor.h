/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2003-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#ifndef Fennel_AttributeAccessor_Included
#define Fennel_AttributeAccessor_Included

FENNEL_BEGIN_NAMESPACE

class TupleAccessor;
class TupleDatum;

/**
 * AttributeAccessor defines how to efficiently unmarshal the value of an
 * attribute from a stored tuple.  Derived classes implement various strategies
 * depending on the storage type.
 *
 *<p>
 *
 * All data members are defined at this level rather than in derived classes,
 * which only provide method implementations.  This makes it possible to write
 * non-polymorphic access code in cases where the entire tuple is being
 * processed, but polymorphic access code in cases where only a small subset of
 * the attributes are being processed.  In theory, this hybrid should yield the
 * highest efficiency, but it needs to be benchmarked and tuned.
 */
class AttributeAccessor
{
public:
    /**
     * Index of this attribute's null indicator bit in the tuple's
     * bit array, or MAXU for a NOT NULL attribute.
     */
    uint iNullBit;

    /**
     * Byte offset of this attribute within a stored tuple image,
     * or MAXU if the start is variable.
     */
    uint iFixedOffset;

    /**
     * Indirect offset of the end of this attribute within a stored
     * tuple image, or MAXU if the end is fixed.
     */
    uint iEndIndirectOffset;

    /**
     * Index of this attribute's value in the tuple's bit array, or
     * MAXU for a non-bit attribute.
     */
    uint iValueBit;

    /**
     * Copied from TupleAttributeDescriptor.cbStorage.  This is not used
     * for anything except assertions.
     */
    TupleStorageByteLength cbStorage;

    virtual ~AttributeAccessor();

    /**
     * Unmarshals the attribute's value by setting up the
     * data pointer, length, and null indicator; does not actually copy any
     * data.
     *
     * @param tupleAccessor containing TupleAccessor set up
     * with the current tuple image to be accessed
     *
     * @param value receives the reference to the unmarshalled value
     */
    virtual void unmarshalValue(
        TupleAccessor const &tupleAccessor,
        TupleDatum &value) const = 0;

    /**
     * Marshals value data for the attribute.  Only deals with the
     * data bytes, not length and null indicators.
     *
     * @param pDestData the target address where the data should be marshalled
     *
     * @param value the value to be marshalled
     */
    virtual void marshalValueData(
        PBuffer pDestData,
        TupleDatum const &value) const = 0;
};

FENNEL_END_NAMESPACE

#endif

// End AttributeAccessor.h
