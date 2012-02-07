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

#ifndef Fennel_VMAllocator_Included
#define Fennel_VMAllocator_Included

#include "fennel/cache/CacheAllocator.h"

FENNEL_BEGIN_NAMESPACE

/**
 * VMAllocator is an implementation of the CacheAllocator interface in
 * terms of OS page allocation calls.
 */
class FENNEL_CACHE_EXPORT VMAllocator
    : public CacheAllocator
{
    size_t cbAlloc;
    size_t nAllocs;
    bool bLockPages;

public:
    /**
     * Constructs a new VMAllocator.
     *
     * @param cbAlloc number of bytes per allocation; all calls
     * to allocate must specify this same value
     *
     * @param nLocked if nonzero, specifies that allocations should be
     * locked in memory, and that no more than the specified number
     * will be allocated at any one time
     */
    explicit VMAllocator(size_t cbAlloc, size_t nLocked = 0);

    virtual ~VMAllocator();

// ----------------------------------------------------------------------
// Implementation of CacheAllocator interface
// ----------------------------------------------------------------------
    virtual void *allocate(int *pErrorCode = NULL);
    virtual int deallocate(void *pMem, int *pErrorCode = NULL);
    virtual int setProtection(
        void *pMem, uint cb, bool readOnly, int *pErrorCode = NULL);
    virtual size_t getBytesAllocated() const;
};

FENNEL_END_NAMESPACE

#endif

// End VMAllocator.h
