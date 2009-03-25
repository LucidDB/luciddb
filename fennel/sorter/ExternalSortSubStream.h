/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2004-2009 LucidEra, Inc.
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

#ifndef Fennel_ExternalSubStream_Included
#define Fennel_ExternalSubStream_Included

FENNEL_BEGIN_NAMESPACE

class ExternalSortFetchArray;

/**
 * ExternalSortRC defines the internal return code for various
 * operations.
 */
enum ExternalSortRC
{
    /**
     * Operation produced valid results.
     */
    EXTSORT_SUCCESS,

    /**
     * Operation produced no results because end of data was reached.
     */
    EXTSORT_ENDOFDATA,

    /**
     * Operation produced some results but could not continue because
     * output area (e.g. generated run) became full.
     */
    EXTSORT_OVERFLOW,
};

/**
 * Fetch interface implemented by sorter subcomponents which return
 * intermediate results.
 */
class ExternalSortSubStream
{
public:
    virtual ~ExternalSortSubStream()
    {
    }

    /**
     * Binds the fetch array which will be used implicitly by
     * subsequent calls to fetch().
     *
     * @return bound fetch array
     */
    virtual ExternalSortFetchArray &bindFetchArray() = 0;

    /**
     * Fetches tuples via the previously bound fetch array.
     *
     * @param nTuplesRequested maximum number of tuples to be returned from
     * fetch (actual count may be less at callee's discretion; this does not
     * indicate end of stream)
     *
     * @return result of fetch (either EXTSORT_ENDOFDATA or EXTSORT_SUCCESS)
     */
    virtual ExternalSortRC fetch(uint nTuplesRequested) = 0;
};

/**
 * Data structure used for array fetch when reading from substreams.
 */
struct ExternalSortFetchArray
{
    /**
     * Array of pointers to marshalled tuple images.
     */
    PBuffer *ppTupleBuffers;

    /**
     * Number of valid entries in ppTupleBuffers.
     */
    uint nTuples;

    /**
     * Creates a new fetch array, initially empty.
     */
    explicit ExternalSortFetchArray()
    {
        ppTupleBuffers = NULL;
        nTuples = 0;
    }
};

/**
 * Maximum number of pointers to return per substream fetch.
 */
const uint EXTSORT_FETCH_ARRAY_SIZE = 100;

FENNEL_END_NAMESPACE

#endif

// End ExternalSortSubStream.h
