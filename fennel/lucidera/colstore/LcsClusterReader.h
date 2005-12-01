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

#ifndef Fennel_LcsClusterReader_Included
#define Fennel_LcsClusterReader_Included

#include "fennel/lucidera/colstore/LcsClusterAccessBase.h"
#include "fennel/lucidera/colstore/LcsClusterNode.h"
#include "fennel/btree/BTreeReader.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LcsClusterReader reads cluster pages
 */
class LcsClusterReader : public LcsClusterAccessBase
{
    /**
     * Reads btrees corresponding to cluster
     */
    SharedBTreeReader bTreeReader;

    /**
     * Reads a cluster page based on current btree position
     *
     * @return cluster page read
     */
    LcsClusterNode const &readClusterPage();

public:
    explicit LcsClusterReader(BTreeDescriptor &treeDescriptor);

    /**
     * Gets first page in a cluster
     *
     * @param pBlock output param returning cluster page
     * 
     * @return true if page available
     */
    bool getFirstClusterPageForRead(PConstLcsClusterNode &pBlock);

    /**
     * Gets next page in a cluster, based on current position in btree
     *
     * @param pBlock output param returning cluster page
     * 
     * @return true if page available
     */
    bool getNextClusterPageForRead(PConstLcsClusterNode &pBlock);
};

FENNEL_END_NAMESPACE

#endif

// End LcsClusterReader.h
