/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_TableWriterFactory_Included
#define Fennel_TableWriterFactory_Included

#include "fennel/txn/LogicalTxnParticipantFactory.h"
#include "fennel/xo/TableWriter.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

class LogicalTxnParticipant;
class StoredTypeDescriptorFactory;
class TableIndexWriterParams;

/**
 * TableWriterFactory implements the LogicalTxnParticipantFactory interface
 * by constructing TableWriters to be used for recovery.  It also implements
 * online pooling, currently for a single txn at a time.
 */
class TableWriterFactory : public LogicalTxnParticipantFactory
{
    SharedSegmentMap pSegmentMap;
    SharedCacheAccessor pCacheAccessor;
    StoredTypeDescriptorFactory const &typeFactory;
    std::vector<SharedTableWriter> pool;
    SegmentAccessor scratchAccessor;

    void loadIndex(
        TupleDescriptor const &,TableIndexWriterParams &,ByteInputStream &);
    
public:
    explicit TableWriterFactory(
        SharedSegmentMap pSegmentMap,
        SharedCacheAccessor pCacheAccessor,
        StoredTypeDescriptorFactory const &typeFactory,
        SegmentAccessor scratchAccessor);

    SharedTableWriter newTableWriter(TableWriterParams const &params);
    
    // implement the LogicalTxnParticipantFactory interface
    virtual SharedLogicalTxnParticipant loadParticipant(
        LogicalTxnClassId classId,
        ByteInputStream &logStream);
    
    static LogicalTxnClassId getParticipantClassId();
};

FENNEL_END_NAMESPACE

#endif

// End TableWriterFactory.h
