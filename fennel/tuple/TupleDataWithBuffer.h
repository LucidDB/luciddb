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

#ifndef Fennel_TupleDataWithBuffer_Included
#define Fennel_TupleDataWithBuffer_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"

#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * TupleDataWithBuffer is a convenience that creates a TupleData, and
 * a supporting buffer from a TupleDescriptor.
 *
 * A common use is to create an input and output tuple for Calculator
 * given the TupleDescriptor obtained from
 * Calculator::getOutputRegisterDescriptor and from
 * Calculator::getInputRegisterDescriptor()
 *
 */
class FENNEL_TUPLE_EXPORT TupleDataWithBuffer
    : public TupleData
{
public:
    explicit TupleDataWithBuffer();
    explicit TupleDataWithBuffer(TupleDescriptor const& tupleDesc);
    void computeAndAllocate(TupleDescriptor const& tupleDesc);
    void resetBuffer();
    ~TupleDataWithBuffer();
private:
    TupleAccessor tupleAccessor;
    boost::scoped_array<FixedBuffer> array;
};

FENNEL_END_NAMESPACE

#endif
// End TupleDataWithBuffer.h
