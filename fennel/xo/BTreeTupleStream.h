/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_BTreeTupleStream_Included
#define Fennel_BTreeTupleStream_Included

#include "fennel/xo/TupleStream.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/btree/BTreeDescriptor.h"

FENNEL_BEGIN_NAMESPACE

class BTreeRootMap;

/**
 * BTreeStreamParams defines parameters common to implementations of
 * BTreeTupleStream.
 */
struct BTreeStreamParams : public TupleStreamParams
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

    explicit BTreeStreamParams();
};

class BTreeRootMap
{
public:
    virtual ~BTreeRootMap();
    virtual PageId getRoot(PageOwnerId pageOwnerId) = 0;
};

class BTreeAccessBase;

/**
 * BTreeTupleStream is a common base for BTree access XO's.
 */
class BTreeTupleStream : virtual public TupleStream
{
protected:
    BTreeDescriptor treeDescriptor;
    SegmentAccessor scratchAccessor;
    BTreeRootMap *pRootMap;
    SharedBTreeAccessBase pBTreeAccessBase;
    
    SharedBTreeReader newReader();
    SharedBTreeWriter newWriter();
    static void copyParamsToDescriptor(
        BTreeDescriptor &,BTreeStreamParams const &);

public:
    void prepare(BTreeStreamParams const &params);
    virtual void open(bool restart);
    virtual void closeImpl();
    virtual BufferProvision getResultBufferProvision() const;

    static SharedBTreeWriter newWriter(BTreeStreamParams const &params);
};

FENNEL_END_NAMESPACE

#endif

// End BTreeTupleStream.h
