/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/

#ifndef Fennel_TupleData_Included
#define Fennel_TupleData_Included

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
    
    explicit TupleDatum();

    void copyFrom(TupleDatum const &other)
    {
        cbData = other.cbData;
        pData = other.pData;
    }
    
    TupleDatum(TupleDatum const &other)
    {
        copyFrom(other);
    }

    TupleDatum &operator = (TupleDatum const &other)
    {
        copyFrom(other);
        return *this;
    }
};

/**
 * TupleData is an in-memory collection of independent data values, as
 * explained in <a href="structTupleDesign.html#TupleData">the design docs</a>.
 */
class TupleData : public std::vector<TupleDatum>
{
public:
    explicit TupleData();
    explicit TupleData(TupleDescriptor const &tupleDesc);

    void compute(TupleDescriptor const &);

    bool containsNull() const;

    /** project unmarshalled data; like TupleDescriptor::projectFrom */
    void projectFrom(TupleData const& src, TupleProjection const&);
};

FENNEL_END_NAMESPACE

#endif

// End TupleData.h
