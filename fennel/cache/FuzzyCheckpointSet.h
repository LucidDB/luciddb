/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

#ifndef Fennel_FuzzyCheckpointSet_Included
#define Fennel_FuzzyCheckpointSet_Included

#include "fennel/cache/PagePredicate.h"

#include <hash_set>
#include <vector>

FENNEL_BEGIN_NAMESPACE

/**
 * FuzzyCheckpointSet keeps track of dirty pages at the time of a checkpoint.
 * It implements the PagePredicate interface by returning true only for pages
 * which have remained dirty since the last checkpoint.
 */
class FuzzyCheckpointSet : public PagePredicate
{
    /**
     * Pages dirty during last checkpoint.
     */
    std::hash_set<BlockId> oldDirtyPages;

    /**
     * Pages dirty during current checkpoint.
     */
    std::vector<BlockId> newDirtyPages;

    PagePredicate *pDelegatePagePredicate;
    
public:
    /**
     * Constructs a new FuzzyCheckpointSet.
     */
    explicit FuzzyCheckpointSet();
    
    /**
     * Forget all dirty pages.
     */
    void clear();

    /**
     * Receives notification that a checkpoint is completing.
     */
    void finishCheckpoint();

    void setDelegatePagePredicate(PagePredicate &pagePredicate);

    // implement the PagePredicate interface
    virtual bool operator()(CachePage const &page);
};

FENNEL_END_NAMESPACE

#endif

// End FuzzyCheckpointSet.h
