/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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

#ifndef Fennel_LcsClusterAccessBase_Included
#define Fennel_LcsClusterAccessBase_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/lucidera/colstore/LcsClusterNode.h"
#include "fennel/btree/BTreeDescriptor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LcsClusterAccessBase is a base for classes which access cluster pages.
 */
class LcsClusterAccessBase
{
protected:
    /**
     * Tuple data representing the btree key corresponding to the cluster
     * page
     */
    TupleData bTreeTupleData;

    /**
     * Accessor for segment storing both btree and cluster pages.
     */
    SegmentAccessor segmentAccessor;
   
    /**
     * Buffer lock for the actual cluster node pages.  Shares the same 
     * segment as the btree corresponding to the cluster.
     */
    ClusterPageLock clusterLock;

    /**
     * Current cluster pageid
     */
    PageId clusterPageId;

    /**
     * Current rid in btree used to access current cluster page
     */
    LcsRid bTreeRid;

    /**
     * Returns RID from btree tuple
     */
    LcsRid readRid();
    
    /**
     * Returns cluster pageid from btree tuple
     */
    PageId readClusterPageId();

public:
    explicit LcsClusterAccessBase(BTreeDescriptor const &treeDescriptor);
};

FENNEL_END_NAMESPACE

#endif

// End LcsClusterAccessBase.h
