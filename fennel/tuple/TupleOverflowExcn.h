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

#ifndef Fennel_TupleOverflowExcn_Included
#define Fennel_TupleOverflowExcn_Included

#include "fennel/common/FennelExcn.h"

FENNEL_BEGIN_NAMESPACE

class TupleDescriptor;
class TupleData;

/**
 * Exception class to be thrown when an oversized tuple is encountered.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_TUPLE_EXPORT TupleOverflowExcn
    : public FennelExcn
{
public:
    /**
     * Constructs a new TupleOverflowExcn.
     *
     *<p>
     *
     * @param tupleDesc descriptor for the tuple
     *
     * @param tupleData data for the tuple
     *
     * @param cbActual actual number of bytes required to store tuple
     *
     * @param cbMax maximum number of bytes available to store tuple
     */
    explicit TupleOverflowExcn(
        TupleDescriptor const &tupleDesc,
        TupleData const &tupleData,
        uint cbActual,
        uint cbMax);
};

FENNEL_END_NAMESPACE

#endif

// End TupleOverflowExcn.h
