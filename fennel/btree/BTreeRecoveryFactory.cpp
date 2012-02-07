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

#include "fennel/common/CommonPreamble.h"
#include "fennel/btree/BTreeRecoveryFactory.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/common/ByteInputStream.h"
#include "fennel/tuple/TupleDescriptor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreeRecoveryFactory::BTreeRecoveryFactory(
    SegmentAccessor segmentAccessortInit,
    SegmentAccessor scratchAccessortInit,
    StoredTypeDescriptorFactory const &typeFactoryInit)
    : segmentAccessor(segmentAccessortInit),
      scratchAccessor(scratchAccessortInit),
      typeFactory(typeFactoryInit)
{
}

SharedLogicalTxnParticipant BTreeRecoveryFactory::loadParticipant(
    LogicalTxnClassId classId,
    ByteInputStream &logStream)
{
    assert(classId == getParticipantClassId());
    BTreeDescriptor descriptor;
    logStream.readValue(descriptor.rootPageId);
    descriptor.segmentAccessor = segmentAccessor;
    descriptor.tupleDescriptor.readPersistent(logStream, typeFactory);
    descriptor.keyProjection.readPersistent(logStream);

    SharedLogicalTxnParticipant pParticipant = writerMap[descriptor.rootPageId];
    if (pParticipant) {
        return pParticipant;
    }

    pParticipant = SharedBTreeWriter(
        new BTreeWriter(
            descriptor, scratchAccessor));

    writerMap[descriptor.rootPageId] = pParticipant;

    return pParticipant;
}

LogicalTxnClassId BTreeRecoveryFactory::getParticipantClassId()
{
    return LogicalTxnClassId(0x80890de8a406fdedLL);
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeRecoveryFactory.cpp
