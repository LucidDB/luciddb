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

#ifndef Fennel_TupleData_Included
#define Fennel_TupleData_Included

#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleDescriptor.h"

#include <vector>


FENNEL_BEGIN_NAMESPACE


class TupleDescriptor;
class TupleProjection;

/**
 * A TupleDatum is a component of TupleData; see
 * <a href="structTupleDesign.html#TupleData">the design docs</a> for
 * more details.
 */
struct FENNEL_TUPLE_EXPORT TupleDatum
{
    TupleStorageByteLength cbData;
    PConstBuffer pData;

    union
    {
        uint16_t data16;
        uint32_t data32;
        uint64_t data64;
    };

    inline explicit TupleDatum();
    inline TupleDatum(TupleDatum const &other);

    /*
     * Test if this TupleDatum represents NULL value.
     *
     * @return true if this TupleDatum represents NULL.
     */
    inline bool isNull() const;

    /**
     * Set this TupleDatum to a NULL value.
     */
    inline void setNull();

    /**
     * Copy assignment(shallow copy).
     *
     * @note
     * See the note of copyFrom method.
     *
     * @param [in] other the source TupleDatum
     */
    inline TupleDatum &operator = (TupleDatum const &other);

    /**
     * Copies data from source(shallow copy).
     *
     * @note
     * pData is set to the source data buffer. If pData points to any buffer
     * before this call, it will no longer point to that buffer after this
     * function call.
     *
     * @param [in] other the source TupleDatum
     */
    inline void copyFrom(TupleDatum const &other);

    /**
     * Copies data into TupleDatum's private buffer(deep copy).
     *
     * @note
     * pData must point to allocated memory before calling this function. If
     * this TupleDatum is part of a TupleDataWithBuffer class,  pData will
     * point to valid memory after computeAndAllocate if no memory has been
     * allocated yet, or resetBuffer if computeAndAllocate had previously been
     * called. If this TupleDatum is not part of a TupleDataWithBuffer class,
     * the caller needs to allocate buffer and set up the pData pointer
     * manually.
     *
     * @par
     * Upon return, pData might no longer point to allocated memory if the
     * source has a null data pointer.
     *
     * @param [in] other the source TupleDatum
     */
    void memCopyFrom(TupleDatum const &other);
};

/**
 * TupleData is an in-memory collection of independent data values, as
 * explained in <a href="structTupleDesign.html#TupleData">the design docs</a>.
 */
class FENNEL_TUPLE_EXPORT TupleData
    : public std::vector<TupleDatum>
{
public:
    inline explicit TupleData();
    inline explicit TupleData(TupleDescriptor const &tupleDesc);

    void compute(TupleDescriptor const &);

    bool containsNull() const;

    bool containsNull(TupleProjection const &tupleProj) const;

    /** project unmarshalled data; like TupleDescriptor::projectFrom */
    void projectFrom(TupleData const& src, TupleProjection const&);
};

/****************************************************
  Definitions of inline methods for class TupleDatum
 ****************************************************/

inline TupleDatum::TupleDatum()
{
    cbData = 0;
    pData = NULL;
}

inline TupleDatum::TupleDatum(TupleDatum const &other)
{
    copyFrom(other);
}

inline bool TupleDatum::isNull() const
{
    return (!pData);
}

inline void TupleDatum::setNull()
{
    pData = NULL;
}

inline TupleDatum &TupleDatum::operator = (TupleDatum const &other)
{
    copyFrom(other);
    return *this;
}

inline void TupleDatum::copyFrom(TupleDatum const &other)
{
    cbData = other.cbData;
    pData = other.pData;
}

/***************************************************
  Definitions of inline methods for class TupleData
 ***************************************************/

inline TupleData::TupleData()
{
}

inline TupleData::TupleData(TupleDescriptor const &tupleDesc)
{
    compute(tupleDesc);
}

FENNEL_END_NAMESPACE

#endif

// End TupleData.h
