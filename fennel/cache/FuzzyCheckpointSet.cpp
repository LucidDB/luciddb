/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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
#include "fennel/cache/FuzzyCheckpointSet.h"

FENNEL_BEGIN_CPPFILE("$Id$");

FuzzyCheckpointSet::FuzzyCheckpointSet()
{
    pDelegatePagePredicate = NULL;
}

void FuzzyCheckpointSet::clear()
{
    oldDirtyPages.clear();
    newDirtyPages.clear();
    pDelegatePagePredicate = NULL;
}

void FuzzyCheckpointSet::finishCheckpoint()
{
    oldDirtyPages.clear();
    oldDirtyPages.insert(
        newDirtyPages.begin(),
        newDirtyPages.end());
    newDirtyPages.clear();
    pDelegatePagePredicate = NULL;
}

bool FuzzyCheckpointSet::operator()(CachePage const &page)
{
    if (!page.isDirty()) {
        return false;
    }
    if (pDelegatePagePredicate && !(*pDelegatePagePredicate)(page)) {
        return false;
    }
    BlockId blockId = page.getBlockId();
    newDirtyPages.push_back(blockId);
    return (oldDirtyPages.find(blockId) != oldDirtyPages.end());
}

void FuzzyCheckpointSet::setDelegatePagePredicate(PagePredicate &pagePredicate)
{
    pDelegatePagePredicate = &pagePredicate;
}

FENNEL_END_CPPFILE("$Id$");

// End FuzzyCheckpointSet.cpp
