/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/btree/BTreeWriter.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeExecStream::prepare(BTreeExecStreamParams const &params)
{
    SingleOutputExecStream::prepare(params);

    copyParamsToDescriptor(treeDescriptor,params);
    scratchAccessor = params.scratchAccessor;
    pRootMap = params.pRootMap;
}

void BTreeExecStream::open(bool restart)
{
    SingleOutputExecStream::open(restart);
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

void BTreeExecStream::closeImpl()
{
    if (pRootMap && pBTreeAccessBase) {
        treeDescriptor.rootPageId = NULL_PAGE_ID;
        pBTreeAccessBase->setRootPageId(NULL_PAGE_ID);
    }
    SingleOutputExecStream::closeImpl();
}

SharedBTreeReader BTreeExecStream::newReader()
{
    SharedBTreeReader pReader = SharedBTreeReader(
        new BTreeReader(treeDescriptor));
    pBTreeAccessBase = pReader;
    return pReader;
}

SharedBTreeWriter BTreeExecStream::newWriter()
{
    SharedBTreeWriter pWriter = SharedBTreeWriter(
        new BTreeWriter(treeDescriptor,scratchAccessor));
    pBTreeAccessBase = pWriter;
    return pWriter;
}

SharedBTreeWriter BTreeExecStream::newWriter(BTreeExecStreamParams const &params)
{
    BTreeDescriptor treeDescriptor;
    copyParamsToDescriptor(treeDescriptor,params);
    return SharedBTreeWriter(
        new BTreeWriter(
            treeDescriptor,params.scratchAccessor));
}

void BTreeExecStream::copyParamsToDescriptor(
    BTreeDescriptor &treeDescriptor,
    BTreeExecStreamParams const &params)
{
    treeDescriptor.segmentAccessor.pSegment = params.pSegment;
    treeDescriptor.segmentAccessor.pCacheAccessor = params.pCacheAccessor;
    treeDescriptor.tupleDescriptor = params.tupleDesc;
    treeDescriptor.keyProjection = params.keyProj;
    treeDescriptor.rootPageId = params.rootPageId;
    treeDescriptor.segmentId = params.segmentId;
    treeDescriptor.pageOwnerId = params.pageOwnerId;
}

BTreeExecStreamParams::BTreeExecStreamParams()
{
    pRootMap = NULL;
}

BTreeOwnerRootMap::~BTreeOwnerRootMap()
{
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeExecStream.cpp
