/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
class BTreeRecoveryFactory : public LogicalTxnParticipantFactory
{
    SegmentAccessor segmentAccessor;
    SegmentAccessor scratchAccessor;
    StoredTypeDescriptorFactory const &typeFactory;

    std::hash_map<PageId,SharedLogicalTxnParticipant> writerMap;

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
