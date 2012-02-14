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

#ifndef Fennel_FtrsTableWriterFactory_Included
#define Fennel_FtrsTableWriterFactory_Included

#include "fennel/txn/LogicalTxnParticipantFactory.h"
#include "fennel/ftrs/FtrsTableWriter.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

class LogicalTxnParticipant;
class StoredTypeDescriptorFactory;
class FtrsTableIndexWriterParams;

/**
 * FtrsTableWriterFactory implements the LogicalTxnParticipantFactory interface
 * by constructing FtrsTableWriters to be used for recovery.  It also implements
 * online pooling, currently for a single txn at a time.
 */
class FENNEL_FTRS_EXPORT FtrsTableWriterFactory
    : public LogicalTxnParticipantFactory
{
    SharedSegmentMap pSegmentMap;
    SharedCacheAccessor pCacheAccessor;
    StoredTypeDescriptorFactory const &typeFactory;
    std::vector<SharedFtrsTableWriter> pool;
    SegmentAccessor scratchAccessor;

    void loadIndex(
        TupleDescriptor const &,FtrsTableIndexWriterParams &,ByteInputStream &);

public:
    explicit FtrsTableWriterFactory(
        SharedSegmentMap pSegmentMap,
        SharedCacheAccessor pCacheAccessor,
        StoredTypeDescriptorFactory const &typeFactory,
        SegmentAccessor scratchAccessor);

    SharedFtrsTableWriter newTableWriter(FtrsTableWriterParams const &params);

    // implement the LogicalTxnParticipantFactory interface
    virtual SharedLogicalTxnParticipant loadParticipant(
        LogicalTxnClassId classId,
        ByteInputStream &logStream);

    static LogicalTxnClassId getParticipantClassId();
};

FENNEL_END_NAMESPACE

#endif

// End FtrsTableWriterFactory.h
