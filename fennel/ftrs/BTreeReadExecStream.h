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

#ifndef Fennel_BTreeReadExecStream_Included
#define Fennel_BTreeReadExecStream_Included

#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleProjectionAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeReadExecStreamParams defines parameters for instantiating a
 * BTreeReadExecStream.
 */
struct FENNEL_FTRS_EXPORT BTreeReadExecStreamParams
    : public BTreeExecStreamParams
{
    /**
     * Projection of attributes to be retrieved from BTree (relative to
     * tupleDesc).
     */
    TupleProjection outputProj;
};

/**
 * BTreeReadExecStream is an abstract base class for ExecStream
 * implementations which project a stream of tuples via a BTreeReader.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_FTRS_EXPORT BTreeReadExecStream
    : public BTreeExecStream
{
protected:
    SharedBTreeReader pReader;
    TupleProjectionAccessor projAccessor;
    TupleData tupleData;
    TupleProjection outputProj;

public:
    // implement ExecStream
    virtual void prepare(BTreeReadExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End BTreeReadExecStream.h
