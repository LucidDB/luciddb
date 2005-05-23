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

#include "fennel/common/CommonPreamble.h"
#include "fennel/xo/BTreeTupleStream.h"
#include "fennel/btree/BTreeWriter.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeTupleStream::prepare(BTreeStreamParams const &params)
{
    TupleStream::prepare(params);

    copyParamsToDescriptor(treeDescriptor,params);
    scratchAccessor = params.scratchAccessor;
    pRootMap = params.pRootMap;
}

void BTreeTupleStream::open(bool restart)
{
    TupleStream::open(restart);
    if (!restart) {
        if (pRootMap) {
            treeDescriptor.rootPageId = pRootMap->getRoot(
                treeDescriptor.pageOwnerId);
            if (pBTreeAccessBase) {
                pBTreeAccessBase->setRootPageId(treeDescriptor.rootPageId);
            }
        }
    }
}

void BTreeTupleStream::closeImpl()
{
    if (pRootMap && pBTreeAccessBase) {
        treeDescriptor.rootPageId = NULL_PAGE_ID;
        pBTreeAccessBase->setRootPageId(NULL_PAGE_ID);
    }
    TupleStream::closeImpl();
}

SharedBTreeReader BTreeTupleStream::newReader()
{
    SharedBTreeReader pReader = SharedBTreeReader(
        new BTreeReader(treeDescriptor));
    pBTreeAccessBase = pReader;
    return pReader;
}

SharedBTreeWriter BTreeTupleStream::newWriter()
{
    SharedBTreeWriter pWriter = SharedBTreeWriter(
        new BTreeWriter(treeDescriptor,scratchAccessor));
    pBTreeAccessBase = pWriter;
    return pWriter;
}

SharedBTreeWriter BTreeTupleStream::newWriter(BTreeStreamParams const &params)
{
    BTreeDescriptor treeDescriptor;
    copyParamsToDescriptor(treeDescriptor,params);
    return SharedBTreeWriter(
        new BTreeWriter(
            treeDescriptor,params.scratchAccessor));
}

void BTreeTupleStream::copyParamsToDescriptor(
    BTreeDescriptor &treeDescriptor,BTreeStreamParams const &params)
{
    treeDescriptor.segmentAccessor.pSegment = params.pSegment;
    treeDescriptor.segmentAccessor.pCacheAccessor = params.pCacheAccessor;
    treeDescriptor.tupleDescriptor = params.tupleDesc;
    treeDescriptor.keyProjection = params.keyProj;
    treeDescriptor.rootPageId = params.rootPageId;
    treeDescriptor.segmentId = params.segmentId;
    treeDescriptor.pageOwnerId = params.pageOwnerId;
}

TupleStream::BufferProvision
BTreeTupleStream::getResultBufferProvision() const
{
    return CONSUMER_PROVISION;
}

BTreeStreamParams::BTreeStreamParams()
{
    pRootMap = NULL;
}

BTreeRootMap::~BTreeRootMap()
{
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeTupleStream.cpp
