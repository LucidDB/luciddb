/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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

#ifndef Fennel_LcsClusterVerifier_Included
#define Fennel_LcsClusterVerifier_Included

#include "fennel/lucidera/colstore/LcsClusterReader.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Information about the cluster page
 */
struct ClusterPageData
{
    /**
     * cluster pageid
     */
    PageId clusterPageId;

    /**
     * rid stored in btree, used to access page
     */
    LcsRid bTreeRid;
};

/**
 * LcsClusterVerifier is a class for verifying cluster pages.  It reads
 * cluster pages and gathers data regarding the page.
 */
class LcsClusterVerifier : public LcsClusterReader
{
    /**
     * Information gathered for the current cluster page
     */
    ClusterPageData pageData;

public:
    explicit LcsClusterVerifier(BTreeDescriptor const &treeDescriptor);
 
    /**
     * Retrieves cluster page data for the current cluster page
     */
    ClusterPageData &getPageData();
};

FENNEL_END_NAMESPACE

#endif

// End LcsClusterVerifier.h
