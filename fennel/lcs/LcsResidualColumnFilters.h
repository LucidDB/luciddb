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
