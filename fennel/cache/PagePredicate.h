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

#ifndef Fennel_PagePredicate_Included
#define Fennel_PagePredicate_Included

#include "fennel/common/CompoundId.h"
#include "fennel/cache/CachePage.h"

FENNEL_BEGIN_NAMESPACE

class MappedPageListener;

/**
 * Callback class for Cache::checkpointPages.
 */
class FENNEL_CACHE_EXPORT PagePredicate
{
public:
    virtual ~PagePredicate();

    /**
     * Tests the predicate.
     *
     * @param page the page to be considered
     *
     * @return true iff the page satisfies the predicate
     */
    virtual bool operator ()(CachePage const &page) = 0;
};

/**
 * DeviceIdPagePredicate is an implementation of PagePredicate which returns
 * true for pages mapped to a given DeviceId.
 */
class FENNEL_CACHE_EXPORT DeviceIdPagePredicate
    : public PagePredicate
{
    DeviceId deviceId;

public:
    explicit DeviceIdPagePredicate(DeviceId);

    virtual bool operator()(CachePage const &page);
};

/**
 * MappedPageListenerPredicate is an implementation of PagePredicate which
 * returns true for pages with a given MappedPageListener
 */
class FENNEL_CACHE_EXPORT MappedPageListenerPredicate
    : public PagePredicate
{
    MappedPageListener &listener;

public:
    explicit MappedPageListenerPredicate(MappedPageListener &);

    virtual bool operator()(CachePage const &page);
};

FENNEL_END_NAMESPACE

#endif

// End PagePredicate.h
