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

#ifndef Fennel_BTreeSortExecStream_Included
#define Fennel_BTreeSortExecStream_Included

#include "fennel/ftrs/BTreeInsertExecStream.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeSortExecStreamParams defines parameters for instantiating a
 * BTreeSortExecStream.  The rootPageId attribute should always be
 * NULL_PAGE_ID.  Note that when distinctness is
 * DUP_DISCARD, the key should normally be the whole tuple to avoid
 * non-determinism with regards to which tuples are discarded.
 */
struct FENNEL_FTRS_EXPORT BTreeSortExecStreamParams
    : public BTreeInsertExecStreamParams
{
};

/**
 * BTreeSortExecStream sorts its input stream according to a parameterized key
 * and returns the sorted data as its output, using a BTree to accomplish the
 * sort.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_FTRS_EXPORT BTreeSortExecStream
    : public BTreeInsertExecStream
{
    bool sorted;

public:
    // implement ExecStream
    void prepare(BTreeSortExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeSortExecStream.h
