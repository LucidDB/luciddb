/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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
