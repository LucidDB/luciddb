/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
