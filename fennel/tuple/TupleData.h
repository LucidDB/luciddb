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
struct TupleDatum
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
class TupleData : public std::vector<TupleDatum>
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
