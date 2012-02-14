/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
