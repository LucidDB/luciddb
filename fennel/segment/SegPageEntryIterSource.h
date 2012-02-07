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

#ifndef Fennel_SegPageEntryIterSource_Included
#define Fennel_SegPageEntryIterSource_Included

FENNEL_BEGIN_NAMESPACE

/**
 * SegPageEntryIterSource provides the callback method that supplies pre-fetch
 * pageIds for SegPageEntryIter.
 */
template <class EntryT>
class SegPageEntryIterSource
{
public:
    virtual ~SegPageEntryIterSource()
    {
    }

    /**
     * Initializes a specific entry in the pre-fetch queue.
     *
     * @param entry the entry that will be initialized
     */
    virtual void initPrefetchEntry(EntryT &entry)
    {
    }

    /**
     * Retrieves the next pageId to be pre-fetched, also filling in context-
     * specific information associated with the page.
     *
     * @param [in, out] entry the context-specific information to be filled in
     *
     * @param [out] found true if a pageId has been found and should be added
     * to the pre-fetch queue
     *
     * @return the prefetch pageId
     */
    virtual PageId getNextPageForPrefetch(EntryT &entry, bool &found) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End SegPageEntryIterSource.h
