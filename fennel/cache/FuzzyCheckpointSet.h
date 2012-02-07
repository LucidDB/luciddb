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
class FENNEL_CACHE_EXPORT FuzzyCheckpointSet
    : public PagePredicate
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
