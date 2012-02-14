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

#ifndef Fennel_BTreeRecoveryFactory_Included
#define Fennel_BTreeRecoveryFactory_Included

#include "fennel/txn/LogicalTxnParticipantFactory.h"
#include "fennel/segment/SegmentAccessor.h"

#include <hash_map>

FENNEL_BEGIN_NAMESPACE

class LogicalTxnParticipant;
class StoredTypeDescriptorFactory;

/**
 * BTreeRecoveryFactory implements the LogicalTxnParticipantFactory interface
 * by constructing BTreeWriters to be used for recovery.
 */
class FENNEL_BTREE_EXPORT BTreeRecoveryFactory
    : public LogicalTxnParticipantFactory
{
    SegmentAccessor segmentAccessor;
    SegmentAccessor scratchAccessor;
    StoredTypeDescriptorFactory const &typeFactory;

    std::hash_map<PageId, SharedLogicalTxnParticipant> writerMap;

public:
    explicit BTreeRecoveryFactory(
        SegmentAccessor segmentAccessor,
        SegmentAccessor scratchAccessor,
        StoredTypeDescriptorFactory const &typeFactory);

    // implement the LogicalTxnParticipantFactory interface
    virtual SharedLogicalTxnParticipant loadParticipant(
        LogicalTxnClassId classId,
        ByteInputStream &logStream);

    static LogicalTxnClassId getParticipantClassId();
};

FENNEL_END_NAMESPACE

#endif

// End BTreeRecoveryFactory.h
