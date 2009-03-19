/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2009 LucidEra, Inc.
// Copyright (C) 2006-2009 The Eigenbase Project
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

#ifndef Fennel_LcsResidualColumnFilters_Included
#define Fennel_LcsResidualColumnFilters_Included

#include <boost/scoped_array.hpp>
#include <boost/dynamic_bitset.hpp>
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/tuple/UnalignedAttributeAccessor.h"
#include "fennel/common/SearchEndpoint.h"
#include "fennel/common/SharedTypes.h"


FENNEL_BEGIN_NAMESPACE

/**
 * Local data structure for a column filter
 */
struct LcsResidualFilter
{
    /**
     * lower bound directive
     */
    SearchEndpoint lowerBoundDirective;

    /**
     * upper bound directive
     */
    SearchEndpoint upperBoundDirective;

    /**
     * row buffer for the corresponding input row
     */
    boost::scoped_array<FixedBuffer> boundBuf;

    /**
     * tuple for the input rows
     */
    TupleData boundData;
};


struct LcsResidualColumnFilters
{
    /**
     * tuple descriptor for key row
     */
    TupleDescriptor inputKeyDesc;

    /**
     * accessor corresponding to inputKeyDesc
     */
    UnalignedAttributeAccessor attrAccessor;

    /**
     * projection from outputTupleData for the key data
     */
    TupleProjection readerKeyProj;

    /**
     * true iff there are predicates on this column
     */
    bool hasResidualFilters;

    /**
     * projection of input stream for lower bound
     */
    TupleProjection lowerBoundProj;

    /**
     * projection of input stream for upper bound
     */
    TupleProjection upperBoundProj;

    /**
     * contains individual predicate info
     */
    std::vector<SharedLcsResidualFilter> filterData;

    /**
     * True if filterData vector has been initialized
     */
    bool filterDataInitialized;

    /**
     * Values bitmap for compressed batch filtering.
     */
    boost::dynamic_bitset<> filteringBitmap;

    /**
     * TupleData used for building contains bitmap
     */
    TupleDataWithBuffer readerKeyData;
};

typedef LcsResidualColumnFilters *PLcsResidualColumnFilters;

FENNEL_END_NAMESPACE

#endif

// End LcsResidualColumnFilters.h
