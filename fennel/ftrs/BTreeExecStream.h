/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
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

#ifndef Fennel_BTreeExecStream_Included
#define Fennel_BTreeExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/btree/BTreeDescriptor.h"

FENNEL_BEGIN_NAMESPACE

class BTreeRootMap;

/**
 * BTreeExecStreamParams defines parameters common to implementations of
 * BTreeExecStream.
 */
struct BTreeExecStreamParams : virtual public SingleOutputExecStreamParams
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
    BTreeRootMap *pRootMap;

    explicit BTreeExecStreamParams();
};

class BTreeRootMap
{
public:
    virtual ~BTreeRootMap();
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
    BTreeRootMap *pRootMap;
    SharedBTreeAccessBase pBTreeAccessBase;
    
    SharedBTreeReader newReader();
    SharedBTreeWriter newWriter();
    static void copyParamsToDescriptor(
        BTreeDescriptor &,BTreeExecStreamParams const &);
public:
    // implement ExecStream
    virtual void prepare(BTreeExecStreamParams const &params);
    virtual void open(bool restart);
    virtual void closeImpl();

    static SharedBTreeWriter newWriter(BTreeExecStreamParams const &params);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeExecStream.h
