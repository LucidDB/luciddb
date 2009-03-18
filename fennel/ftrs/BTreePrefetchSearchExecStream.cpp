/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
#include "fennel/btree/BTreeNonLeafReader.h"
#include "fennel/btree/BTreeLeafReader.h"
#include "fennel/btree/BTreeNodeAccessor.h"
#include "fennel/segment/SegPageEntryIterImpl.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/ftrs/BTreePrefetchSearchExecStream.h"

#include "math.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BTreePrefetchSearchExecStream::BTreePrefetchSearchExecStream() : leafPageQueue()
{
}

void BTreePrefetchSearchExecStream::prepare(
    BTreePrefetchSearchExecStreamParams const &params)
{
    BTreeSearchExecStream::prepare(params);

    savedLowerBoundAccessor.compute(inputKeyDesc);
    pfLowerBoundData.compute(inputKeyDesc);
    if (upperBoundDesc.size() == 0) {
        savedUpperBoundAccessor.compute(inputKeyDesc);
        pfUpperBoundData.compute(inputKeyDesc);
    } else {
        savedUpperBoundAccessor.compute(upperBoundDesc);
        pfUpperBoundData.compute(upperBoundDesc);
    }

    scratchLock.accessSegment(scratchAccessor);
    scratchPageSize = scratchAccessor.pSegment->getUsablePageSize();
}

void BTreePrefetchSearchExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity,
    ExecStreamResourceSettingType &optType)
{
    BTreeSearchExecStream::getResourceRequirements(minQuantity, optQuantity);

    // Need one more page because there are two btree readers
    minQuantity.nCachePages += 1;
    optQuantity.nCachePages += 1;
    nMiscCachePages = minQuantity.nCachePages;

    // Set aside pages for storing search key information associated with
    // pre-fetched index leaf pages.  At a minimum, we need one page, but ask
    // for extra if it's available, based on the space required for the
    // search key information, up to a max of 8K pre-fetch entries.
    minQuantity.nCachePages += 1;
    keyValuesSize = savedLowerBoundAccessor.getMaxByteCount() * 2;

    // The actual size of the keys should never exceed a page size, but we're
    // trying to store at least 2 keys on each page.  If the max size is such
    // that we can only store a single key on a page, then set aside an entire
    // page to store each key.
    if (keyValuesSize > scratchPageSize) {
        bigMaxKey = true;
        keyValuesSize = scratchPageSize;
    } else {
        bigMaxKey = false;
    }

    nEntriesPerScratchPage = scratchPageSize / keyValuesSize;
    uint optNumPages = (uint) ceil(8192.0 / nEntriesPerScratchPage);
    assert(optNumPages >= 1);
    optQuantity.nCachePages += optNumPages;
    optType = EXEC_RESOURCE_ACCURATE;
}

void BTreePrefetchSearchExecStream::setResourceAllocation(
    ExecStreamResourceQuantity &quantity)
{
    BTreeSearchExecStream::setResourceAllocation(quantity);
    nPrefetchScratchPages = quantity.nCachePages - nMiscCachePages;
}

void BTreePrefetchSearchExecStream::open(bool restart)
{
    BTreeSearchExecStream::open(restart);
    pNonLeafReader =
        SharedBTreeNonLeafReader(new BTreeNonLeafReader(treeDescriptor));
    initialPrefetchDone = false;
    processingDone = false;
    prevLeafSearchRc = true;
    returnedRoot = false;
    rootOnly = (pNonLeafReader->isRootOnly());

    if (!restart) {
        uint nPrefetchEntries =
            (bigMaxKey) ?
                nPrefetchScratchPages / 2 :
                nPrefetchScratchPages * nEntriesPerScratchPage;
        leafPageQueue.resize(nPrefetchEntries);
        allocateScratchPages();
        leafPageQueue.setPrefetchSource(*this);
    }
}

SharedBTreeReader BTreePrefetchSearchExecStream::newReader()
{
    pLeafReader = SharedBTreeLeafReader(new BTreeLeafReader(treeDescriptor));
    pBTreeAccessBase = pBTreeReader = pLeafReader;
    return pBTreeReader;
}

void BTreePrefetchSearchExecStream::allocateScratchPages()
{
    for (uint i = 0; i < nPrefetchScratchPages; i++) {
        scratchLock.allocatePage();
        PBuffer page = scratchLock.getPage().getWritableData();
        scratchPages.push_back(page);
    }
    currPage = 0;
    currPageEntry = 0;
}

void BTreePrefetchSearchExecStream::initPrefetchEntry(
    BTreePrefetchSearchKey &searchKey)
{
    if (bigMaxKey) {
        searchKey.lowerKeyBuffer = scratchPages[currPage++];
        searchKey.upperKeyBuffer = scratchPages[currPage++];
    } else {
        searchKey.lowerKeyBuffer =
            scratchPages[currPage] + currPageEntry * keyValuesSize;
        searchKey.upperKeyBuffer = searchKey.lowerKeyBuffer + keyValuesSize / 2;
        if (++currPageEntry == nEntriesPerScratchPage) {
            currPage++;
            currPageEntry = 0;
        }
    }
}

ExecStreamResult BTreePrefetchSearchExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    uint nTuples = 0;

    // Iterate over each input search key, locating and pre-fetching
    // leaf pages that contain matching keys.
    for (;;) {

        // Position within a pre-fetched leaf page.
        if (!innerSearchLoop()) {
            return EXECRC_BUF_UNDERFLOW;
        }

        // If there are no more search keys in the input and all input has
        // been processed, then we've completed execution.
        if (pInAccessor->getState() == EXECBUF_EOS && processingDone) {
            pOutAccessor->markEOS();
            return EXECRC_EOS;
        }

        // Then, fetch matching records from that leaf page until we hit
        // a record outside the desired search range.
        ExecStreamResult rc = innerFetchLoop(quantum, nTuples);
        if (rc != EXECRC_YIELD) {
            return rc;
        }
    }
}

bool BTreePrefetchSearchExecStream::innerSearchLoop()
{
    // If we're already positioned within a leaf page, then nothing further
    // needs to be done here.
    while (!pReader->isPositioned()) {

        // Make sure there's input available, in case we're going to
        // pre-fetch some pages.
        if (pInAccessor->getState() != EXECBUF_EOS &&
            !pInAccessor->demandData())
        {
            return false;
        }

        if (!initialPrefetchDone) {
            if (pInAccessor->getState() == EXECBUF_EOS) {
                processingDone = true;
                break;
            }
            getPrefetchSearchKey();
            leafPageQueue.mapRange(
                treeDescriptor.segmentAccessor,
                NULL_PAGE_ID);
            initialPrefetchDone = true;
        } else {
            ++leafPageQueue;
        }

        std::pair<PageId, BTreePrefetchSearchKey> &prefetchEntry =
            *leafPageQueue;
        // When we hit the terminating page, then all input search ranges
        // have been processed.
        if (prefetchEntry.first == NULL_PAGE_ID) {
            processingDone = true;
            break;
        }

        // Setup the search directives and keys to match the criteria
        // used to locate the pre-fetch page.  Note that we need to do
        // this even in the case where we don't need to search the
        // page because the directives and bounds are still needed to
        // determine where to end the search.
        setUpSearchKey(prefetchEntry.second);
        pLeafReader->setCurrentPageId(prefetchEntry.first);

        // If the previous leaf search yielded a non-match, then we need
        // to search the current page, even though this is a continuation
        // of a previous search.  Otherwise, just start at the first key
        // in the new page.
        if (prefetchEntry.second.newSearch || !prevLeafSearchRc) {
            prevLeafSearchRc = searchForKey();
            if (prevLeafSearchRc) {
                break;
            }
        } else {
            if (pReader->searchFirst() && testInterval()) {
                projAccessor.unmarshal(tupleData.begin());
                break;
            }
        }
        // If the current leaf page yielded no matches, end the search on it,
        // and loop back to search the next leaf page.
        pReader->endSearch();
    }
    return true;
}

void BTreePrefetchSearchExecStream::getPrefetchSearchKey()
{
    readSearchKey();
    readDirectives();
    setAdditionalKeys();
    readUpperBoundKey();
    pfLowerBoundDirective = lowerBoundDirective;
    pfUpperBoundDirective = upperBoundDirective;
    pfLowerBoundData = *pSearchKey;
    pfUpperBoundData = upperBoundData;
}

void BTreePrefetchSearchExecStream::setUpSearchKey(
    BTreePrefetchSearchKey const &searchKey)
{
    lowerBoundDirective = searchKey.lowerBoundDirective;
    upperBoundDirective = searchKey.upperBoundDirective;
    setLowerBoundKey(searchKey.lowerKeyBuffer);
    savedUpperBoundAccessor.setCurrentTupleBuf(searchKey.upperKeyBuffer);
    savedUpperBoundAccessor.unmarshal(upperBoundData);
}

void BTreePrefetchSearchExecStream::setLowerBoundKey(PConstBuffer buf)
{
    savedLowerBoundAccessor.setCurrentTupleBuf(buf);
    savedLowerBoundAccessor.unmarshal(inputKeyData);
    pSearchKey = &inputKeyData;
}

void BTreePrefetchSearchExecStream::setAdditionalKeys()
{
    pSearchKey = &inputKeyData;
}

PageId BTreePrefetchSearchExecStream::getNextPageForPrefetch(
    BTreePrefetchSearchKey &searchKey,
    bool &found)
{
    found = true;

    // Handle the special case where the tree consists of a single root page
    // by simply pre-fetching that root page for each search range.  Note
    // that the page will actually only be pre-fetched once.
    if (rootOnly) {
        while (true) {
            if (returnedRoot) {
                if (pInAccessor->getState() == EXECBUF_EOS) {
                    return NULL_PAGE_ID;
                } else if (!pInAccessor->demandData()) {
                    found = false;
                    return NULL_PAGE_ID;
                }
                returnedRoot = false;
                getPrefetchSearchKey();
            }
            returnedRoot = true;
            setSearchKeyData(true, searchKey);
            pInAccessor->consumeTuple();
            return pNonLeafReader->getRootPageId();
        }
    }

    bool first;

    while (true) {
        // If we're already positioned within the non-leaf page, then continue
        // searching the page, unless the previous key was larger than the
        // upper bound, in which case, the previous page is the last matching
        // leaf page, and therefore, we should end the current search.
        if (pNonLeafReader->isPositioned()) {
            first = false;
            if (endOnNextKey) {
                pInAccessor->consumeTuple();
                pNonLeafReader->endSearch();
                continue;
            } else {
                pNonLeafReader->searchNext();
            }

        // Otherwise, initiate a search on the non-leaf page based on the lower
        // bound directive.  If necessary, first read the search directives
        // from the input stream, provided there's input available.
        } else {
            if (!pInAccessor->isTupleConsumptionPending()) {
                // If the current input stream buffer is exhausted, passing
                // back a NULL_PAGE_ID will signal innerSearchLoop() to
                // map a new range of pre-fetches.
                if (pInAccessor->getState() == EXECBUF_EOS) {
                    return NULL_PAGE_ID;
                } else if (!pInAccessor->demandData()) {
                    found = false;
                    return NULL_PAGE_ID;
                }
                getPrefetchSearchKey();
            }
            first = true;
            switch (pfLowerBoundDirective) {
            case SEARCH_UNBOUNDED_LOWER:
                if (pfLowerBoundData.size() <= 1) {
                    pNonLeafReader->searchFirst();
                    break;
                }
            // Fall through to handle the case where we have > 1 key and a
            // non-equality search on the last key.  In this case, we need to
            // position to the equality portion of the key
            case SEARCH_CLOSED_LOWER:
                pNonLeafReader->searchForKey(
                    pfLowerBoundData, DUP_SEEK_BEGIN, leastUpper);
                break;
            case SEARCH_OPEN_LOWER:
                pNonLeafReader->searchForKey(
                    pfLowerBoundData, DUP_SEEK_END, leastUpper);
                break;
            default:
                permFail(
                    "unexpected lower bound directive:  "
                    << (char) lowerBoundDirective);
            }
        }

        // See if the key found corresponds to the last possible matching
        // key before passing back the pre-fetch info.  Note that we should
        // always find a key because at a minimum, we'll find the infinity
        // key.
        assert(!pNonLeafReader->isSingular());
        endOnNextKey = testNonLeafInterval();
        setSearchKeyData(first, searchKey);
        return pNonLeafReader->getChildForCurrent();
    }
}

void BTreePrefetchSearchExecStream::setSearchKeyData(
    bool newSearch,
    BTreePrefetchSearchKey &searchKey)
{
    searchKey.newSearch = newSearch;
    searchKey.lowerBoundDirective =
        pfLowerBoundDirective;
    searchKey.upperBoundDirective =
        pfUpperBoundDirective;
    if (bigMaxKey) {
        assert(
            savedLowerBoundAccessor.getByteCount(pfLowerBoundData)
                <= keyValuesSize);
        assert(
            savedUpperBoundAccessor.getByteCount(pfUpperBoundData)
                <= keyValuesSize);
    }
    savedLowerBoundAccessor.marshal(pfLowerBoundData, searchKey.lowerKeyBuffer);
    savedUpperBoundAccessor.marshal(pfUpperBoundData, searchKey.upperKeyBuffer);
}

bool BTreePrefetchSearchExecStream::testNonLeafInterval()
{
    // If the search has positioned all the way to the right-most node, then
    // this is the last non-leaf key.
    if (pNonLeafReader->isPositionedOnInfinityKey()) {
        return true;
    }

    BTreeNodeAccessor &nodeAccessor =
        pNonLeafReader->getNonLeafNodeAccessor();
    if (pfUpperBoundDirective == SEARCH_UNBOUNDED_UPPER) {
        // If there is more one search key in an unbounded search, then the
        // first part of the key is an equality.  In this case, we have
        // to check the equality part of the key to determine whether this
        // will be the last matching key.
        if (pfLowerBoundData.size() > 1) {
            nodeAccessor.unmarshalKey(readerKeyData);
            int c =
                inputKeyDesc.compareTuplesKey(
                    pfLowerBoundData,
                    readerKeyData,
                    pfLowerBoundData.size() - 1);
            // Should never hit c > 0 because the lower bound search should
            // not position on a key that the lower bound is greater than.
            permAssert(c <= 0);
            return (c < 0);
        } else {
            return false;
        }
    } else {
        nodeAccessor.unmarshalKey(readerKeyData);
        int c = inputKeyDesc.compareTuples(pfUpperBoundData, readerKeyData);
        // If the upper bound is less than, or equal to the non-leaf key in
        // an open upper search, then this is the last possible matching key.
        if (c == 0) {
            if (pfUpperBoundDirective == SEARCH_OPEN_UPPER) {
                c = -1;
            }
        }
        return (c < 0);
    }
}

void BTreePrefetchSearchExecStream::closeImpl()
{
    BTreeSearchExecStream::closeImpl();
    scratchPages.clear();
    if (scratchAccessor.pSegment) {
        scratchAccessor.pSegment->deallocatePageRange(
            NULL_PAGE_ID, NULL_PAGE_ID);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End BTreePrefetchSearchExecStream.cpp
