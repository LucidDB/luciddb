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

#ifndef Fennel_LbmAggExecStream_Included
#define Fennel_LbmAggExecStream_Included

#include "fennel/exec/SortedAggExecStream.h"
#include "fennel/exec/AggInvocation.h"
#include "fennel/exec/AggComputer.h"
#include "fennel/lbm/LbmByteSegment.h"
#include "fennel/tuple/TupleDataWithBuffer.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmSortedAggExecStreamParams defines parameters for LbmSortedAggExecStream.
 */
struct LbmSortedAggExecStreamParams : public SortedAggExecStreamParams
{
};

/**
 * LbmSortedAggExecStream aggregates its input, producing tuples with
 * aggregate computations as output. It takes input sorted on a group key
 * and produces one output tuple per group. It is similar to
 * SortedAggExecStream, but it's input consists of a bitmap tuple.
 * (group by keys, other fields, bitmap fields)
 *
 * <p>
 *
 * The behavior of MIN and MAX are not changed by bitmap fields.
 * However, the COUNT and SUM accumulations are applied multiple times,
 * according to the number of bits in the bitmap fields.
 *
 * @author John Pham
 * @version $Id$
 */
class FENNEL_LBM_EXPORT LbmSortedAggExecStream
    : public SortedAggExecStream
{
protected:
    virtual AggComputer *newAggComputer(
        AggFunction aggFunction,
        TupleAttributeDescriptor const *pAttrDesc);

public:
    // implement ExecStream
    virtual void prepare(LbmSortedAggExecStreamParams const &params);
};

FENNEL_END_NAMESPACE

#endif

// End LbmSortedAggExecStream.h
