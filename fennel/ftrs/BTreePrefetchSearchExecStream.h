/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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

#ifndef Fennel_BTreePrefetchSearchExecStream_Included
#define Fennel_BTreePrefetchSearchExecStream_Included

#include "fennel/ftrs/BTreeSearchExecStream.h"
#include "fennel/btree/BTreeNode.h"
#include "fennel/segment/SegPageEntryIter.h"
#include "fennel/segment/SegPageEntryIterSource.h"
#include "fennel/common/SearchEndpoint.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Structure used to store the search key information that was used to locate
 * a pre-fetched btree leaf page.
 */
struct BTreePrefetchSearchKey
{
    /**
     * Lower bound directive of the search key
     */
    SearchEndpoint lowerBoundDirective;

    /**
     * Upper bound directive of the search key
     */
    SearchEndpoint upperBoundDirective;

    /**
     * Buffer used to store lower key value
     */
    PBuffer lowerKeyBuffer;

    /**
     * Buffer used to store upper key value
     */
    PBuffer upperKeyBuffer;

    /**
     * True if the pre-fetched leaf page needs to be searched before it's
     * read.  Otherwise, the first key on the page can be read.
     */
    bool newSearch;
};

/**
 * BTreePrefetchSearchExecStreamParams defines parameters for instantiating a
 * BTreePrefetchSearchExecStream
 */
struct BTreePrefetchSearchExecStreamParams : public BTreeSearchExecStreamParams
{
};

/**
 * BTreePrefetchSearchExecStream extends BTreeSearchExecStream by pre-fetching
 * index leaf pages.  It does this by utilizing two btree readers.  The first
 * searches and reads only non-leaf pages, and initiates the pre-fetching of
 * leaf pages.  The second reader searches and reads the pre-fetched leaf
 * pages.
 *
 * <p>NOTE: Because this execution stream allocates a number of scratch pages
 * that are freed when the stream is closed, a private scratch segment
 * should be created for the stream, if it is used in a stream graph with other
 * streams that also use scratch pages.
 *
 * <p>Also, this stream assumes that it's reading a snapshot of the btree
 * pages.  Specifically, it does not handle the case where the btree grows
 * or shrinks from/to a 1-level btree while the tree is being searched/read.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class BTreePrefetchSearchExecStream :
    public BTreeSearchExecStream,
    public SegPageEntryIterSource<BTreePrefetchSearchKey>
{
protected:
    /**
     * Iterator that provides the pre-fetched leaf pages
     */
    SegPageEntryIter<BTreePrefetchSearchKey> leafPageQueue;

    /**
     * Reader used to search/read leaf pages from the btree.  This shared
     * pointer points to the same object as pReader.
     */
    SharedBTreeLeafReader pLeafReader;

    /**
     * Reader used to search/read non-leaf pages from the btree
     */
    SharedBTreeNonLeafReader pNonLeafReader;

    /**
     * Accessor used to reference a saved lower bound search key that will
     * be used to search a leaf page
     */
    TupleAccessor savedLowerBoundAccessor;

    /**
     * Accessor used to reference a saved upper bound search key used in a
     * leaf page search
     */
    TupleAccessor savedUpperBoundAccessor;

    /**
     * Lower bound directive used when searching non-leaf nodes to pre-fetch
     * leaf pages
     */
    SearchEndpoint pfLowerBoundDirective;

    /**
     * Upper bound directive used when searching non-leaf nodes to pre-fetch
     * leaf pages
     */
    SearchEndpoint pfUpperBoundDirective;

    /**
     * Lower bound key used when searching non-leaf nodes to pre-fetch
     * leaf pages
     */
    TupleData pfLowerBoundData;

    /**
     * Upper bound key used when searching non-leaf nodes to pre-fetch
     * leaf pages
     */
    TupleData pfUpperBoundData;

    /**
     * If true, abort search of non-leaf page when positioning to the
     * next key
     */
    bool endOnNextKey;

    /**
     * Page lock used to allocate scratch pages used to store search key
     * information associated with pre-fetched leaf pages
     */
    BTreePageLock scratchLock;

    /**
     * Space available on a single scratch page
     */
    uint scratchPageSize;

    /**
     * Number of cache pages needed, including pages needed for pre-fetches,
     * but excluding scratch pages used to store search key information
     */
    uint nMiscCachePages;

    /**
     * Number of scratch pages available to store search key information
     */
    uint nPrefetchScratchPages;

    /**
     * Size of space occupied by lower and upper bound key values
     */
    uint keyValuesSize;

    /**
     * Number of search key entries per scratch page
     */
    uint nEntriesPerScratchPage;

    /**
     * True if the maximum key size is such that potentially only a single
     * key can be stored on a page
     */
    bool bigMaxKey;

    /**
     * Vector of pointers to scratch pages allocated
     */
    std::vector<PBuffer> scratchPages;

    /**
     * Index into scratch pages vector, indicating the current page whose
     * space is being used to initialize pre-fetch entries
     */
    uint currPage;

    /**
     * Index into the current scratch page whose space is being used to
     * initialize pre-fetch entries
     */
    uint currPageEntry;

    /**
     * True if first pre-fetch needs to be initiated
     */
    bool initialPrefetchDone;

    /**
     * True if all input search ranges have been processed
     */
    bool processingDone;

    /**
     * Boolean return code from the previous leaf page search
     */
    bool prevLeafSearchRc;

    /**
     * True if the btree is a root-only tree
     */
    bool rootOnly;

    /**
     * True if the root, leaf page has been returned as a pre-fetch page
     * for the current search range
     */
    bool returnedRoot;

    /**
     * Allocates a new BTreeNonLeafReader.
     */
    virtual SharedBTreeReader newReader();

    /**
     * Allocates the scratch pages that will be used to store search key
     * information.
     */
    void allocateScratchPages();

    /**
     * Searches individual leaf pages.
     *
     * @return true if the search was successful; false if the input stream
     * has been exhausted
     */
    bool innerSearchLoop();

    /**
     * Reads the search directives and keys from the input stream, and
     * initializes the data for use in the btree searches used to pre-fetch
     * leaf pages.
     */
    void getPrefetchSearchKey();

    /**
     * Sets up the search key for the current leaf page search using data
     * stored away by a pre-fetch.
     *
     * @param [in, out] searchKey the data used to setup the search key
     */
    void setUpSearchKey(BTreePrefetchSearchKey const &searchKey);

    /**
     * Saves away the search key data corresponding to a pre-fetched page.
     *
     * @param newSearch whether a new search yielded the page, or the page is
     * a continuation of a previous search
     *
     * @param [in, out] searchKey the search key information to be filled in
     */
    void setSearchKeyData(bool newSearch, BTreePrefetchSearchKey &searchKey);

    /**
     * Determines if the current key just read from a non-leaf page contains
     * the last matching page in the search interval.
     *
     * @return true if the current key contains the last matching non-leaf
     * page
     */
    bool testNonLeafInterval();

    /**
     * Sets the search key.
     */
    virtual void setAdditionalKeys();

    /**
     * Sets the lower bound key saved away from a previous pre-fetch.
     *
     * @param buf the buffer containing the lower bound key
     */
    virtual void setLowerBoundKey(PConstBuffer buf);

public:
    explicit BTreePrefetchSearchExecStream();
    virtual void prepare(BTreePrefetchSearchExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity,
        ExecStreamResourceSettingType &optType);
    virtual void setResourceAllocation(ExecStreamResourceQuantity &quantity);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void initPrefetchEntry(BTreePrefetchSearchKey &searchKey);
    virtual PageId getNextPageForPrefetch(
        BTreePrefetchSearchKey &searchKey,
        bool &found);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End BTreePrefetchSearchExecStream.h
