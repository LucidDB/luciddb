/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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

#ifndef Fennel_BTreeExecStream_Included
#define Fennel_BTreeExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/btree/BTreeDescriptor.h"

FENNEL_BEGIN_NAMESPACE

class BTreeOwnerRootMap;

/**
 * BTreeParams defines parameters used when accessing btrees
 */
struct BTreeParams
{
    /**
     * Segment containing BTree.
     */
    SharedSegment pSegment;
    
    /**
     * Root of BTree, or NULL_PAGE_ID for variable root.
     */
    PageId rootPageId;

    /**
     * SegmentId of segment storing BTree.
     */
    SegmentId segmentId;
        
    /**
     * PageOwnerId used to mark pages.
     */
    PageOwnerId pageOwnerId;
    
    /**
     * TupleDescriptor for BTree entries.
     */
    TupleDescriptor tupleDesc;

    /**
     * Key projection for BTree entries (relative to tupleDesc).
     */
    TupleProjection keyProj;

    /**
     * Map for looking up variable index roots, or NULL for permanent root.
     */
    BTreeOwnerRootMap *pRootMap;

    /**
     * Parameter id corresponding to the rootPageId when the btree is
     * dynamically created during execution of the stream graph
     */
    DynamicParamId rootPageIdParamId;
};

/**
 * BTreeExecStreamParams defines parameters common to implementations of
 * BTreeExecStream.
 */
struct BTreeExecStreamParams :
    BTreeParams, virtual public SingleOutputExecStreamParams
{
    explicit BTreeExecStreamParams();
};

class BTreeOwnerRootMap
{
public:
    virtual ~BTreeOwnerRootMap();
    virtual PageId getRoot(PageOwnerId pageOwnerId) = 0;
};

class BTreeAccessBase;

/**
 * BTreeExecStream is a common base for ExecStream implementations which
 * access BTrees.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class BTreeExecStream : virtual public SingleOutputExecStream
{
protected:
    BTreeDescriptor treeDescriptor;
    SegmentAccessor scratchAccessor;
    BTreeOwnerRootMap *pRootMap;
    SharedBTreeAccessBase pBTreeAccessBase;
    SharedBTreeReader pBTreeReader;
    DynamicParamId rootPageIdParamId;
    
    virtual SharedBTreeReader newReader();
    SharedBTreeWriter newWriter(bool monotonic = false);

    /**
     * Forgets the current reader or writer's search, releasing any page locks
     */
    virtual void endSearch();
public:
    // implement ExecStream
    virtual void prepare(BTreeExecStreamParams const &params);
    virtual void open(bool restart);
    virtual void closeImpl();

    static void copyParamsToDescriptor(
        BTreeDescriptor &,
        BTreeParams const &,
        SharedCacheAccessor const &);
    static SharedBTreeWriter newWriter(BTreeExecStreamParams const &params);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeExecStream.h
