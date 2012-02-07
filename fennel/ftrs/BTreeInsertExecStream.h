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

#ifndef Fennel_BTreeInsertExecStream_Included
#define Fennel_BTreeInsertExecStream_Included

#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/exec/ConduitExecStream.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/common/FemEnums.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeInserterParams defines parameters for instantiating a BTreeInserter.
 */
struct FENNEL_FTRS_EXPORT BTreeInsertExecStreamParams
    : public BTreeExecStreamParams, public ConduitExecStreamParams
{
    Distinctness distinctness;
    bool monotonic;
};

/**
 * BTreeInsertExecStream inserts tuples into a BTree, reading them from an
 * upstream stream producer.  The BTree that's written into can optionally
 * be created by the stream itself, in which case, the rootPageId of the
 * dynamically created BTree is written into a dynamic parameter to be read
 * by other streams.  In that case, the BTree is destroyed when the stream
 * is closed.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_FTRS_EXPORT BTreeInsertExecStream
    : public BTreeExecStream, public ConduitExecStream
{
    Distinctness distinctness;
    bool monotonic;

    void buildTree(bool restart);

    void truncateTree(bool rootless);

protected:
    bool dynamicBTree;

    bool truncateOnRestart;

    SharedBTreeWriter pWriter;

    virtual void closeImpl();

public:
    // implement ExecStream
    virtual void prepare(BTreeInsertExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeInsertExecStream.h
