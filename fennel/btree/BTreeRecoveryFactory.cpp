/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
    descriptor.tupleDescriptor.readPersistent(logStream,typeFactory);
    descriptor.keyProjection.readPersistent(logStream);

    SharedLogicalTxnParticipant pParticipant = writerMap[descriptor.rootPageId];
    if (pParticipant) {
        return pParticipant;
    }
    
    pParticipant = SharedBTreeWriter(
        new BTreeWriter(
            descriptor,scratchAccessor));

    writerMap[descriptor.rootPageId] = pParticipant;
    
    return pParticipant;
}

LogicalTxnClassId BTreeRecoveryFactory::getParticipantClassId()
{
    return LogicalTxnClassId(0x80890de8a406fdedLL);
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeRecoveryFactory.cpp
