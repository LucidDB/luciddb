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

#ifndef Fennel_CacheAllocator_Included
#define Fennel_CacheAllocator_Included

FENNEL_BEGIN_NAMESPACE

/**
 * CacheAllocator defines an interface for allocating memory pages to be used
 * by the cache.
 */
class FENNEL_CACHE_EXPORT CacheAllocator
{
public:
    virtual ~CacheAllocator();

    /**
     * Allocates a chunk of memory of size determined by the constructor.
     *
     * @param pErrorCode on error and if non-NULL, the int referenced is
     * modified to contain the OS error code
     *
     * @return the allocated chunk; NULL if memory cannot be allocated (see
     * pErrorCode for OS error code)
     */
    virtual void *allocate(int *pErrorCode = NULL) = 0;

    /**
     * Deallocates a chunk of memory.
     *
     * @param pMem the allocated memory
     *
     * @param pErrorCode on error and if non-NULL, the int referenced is
     * modified to contain the OS error code
     *
     * @return 0 on success; -1 if memory cannot be deallocated (see
     * pErrorCode for OS error code)
     */
    virtual int deallocate(void *pMem, int *pErrorCode = NULL) = 0;

    /**
     * @return number of bytes currently allocated
     */
    virtual size_t getBytesAllocated() const = 0;

    /**
     * Changes protection state for a contiguous range of virtual memory.
     *
     * @param pMem start of range
     *
     * @param cb number of bytes in range
     *
     * @param readOnly true for read-only; false for read-write
     * (TODO jvs 7-Feb-2006:  support no-access as well)
     *
     * @param pErrorCode on error and if non-NULL, the int referenced is
     * modified to contain the OS error code
     *
     * @return 0 on success; -1 if an error occurs while manupulating memory
     * protections (see pErrorCode for OS error code)
     */
    virtual int setProtection(
        void *pMem, uint cb, bool readOnly, int *pErrorCode = NULL) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End CacheAllocator.h
