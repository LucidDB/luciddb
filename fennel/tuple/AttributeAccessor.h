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
class FENNEL_TUPLE_EXPORT AttributeAccessor
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
