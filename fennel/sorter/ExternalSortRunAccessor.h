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

#ifndef Fennel_ExternalSortRunAccessor_Included
#define Fennel_ExternalSortRunAccessor_Included

#include "fennel/tuple/TupleAccessor.h"
#include "fennel/sorter/ExternalSortSubStream.h"

FENNEL_BEGIN_NAMESPACE

class ExternalSortInfo;

/**
 * ExternalSortRunAccessor manages I/O for storing runs and reading them back.
 */
class FENNEL_SORTER_EXPORT ExternalSortRunAccessor
    : public ExternalSortSubStream
{
    /**
     * Global information.
     */
    ExternalSortInfo &sortInfo;

    // TODO:  comment or replace
    TupleAccessor tupleAccessor;

    /**
     * Array used to return fetch results.  This is permanently bound to
     * ppTupleBuffers.
     */
    ExternalSortFetchArray fetchArray;

    /**
     * Pointer array used to return fetch results.  These pointers get bound to
     * contiguous tuples on stored run pages as they are read in.
     */
    PBuffer ppTupleBuffers[EXTSORT_FETCH_ARRAY_SIZE];

    /**
     * Helper used for reading stored runs.
     */
    SharedSegInputStream pSegInputStream;

    /**
     * Helper used for writing stored runs.
     */
    SharedSegOutputStream pSegOutputStream;

    /**
     * Information about run being accessed.
     */
    SharedSegStreamAllocation pStoredRun;

// ----------------------------------------------------------------------
// private methods
// ----------------------------------------------------------------------

    void clearFetch()
    {
        fetchArray.nTuples = 0;
        fetchArray.ppTupleBuffers = ppTupleBuffers;
        memset(ppTupleBuffers, 0, sizeof(ppTupleBuffers));
    }

public:
    explicit ExternalSortRunAccessor(ExternalSortInfo &);
    virtual ~ExternalSortRunAccessor();

    /**
     * Prepares this accessor to read (but does not specify a particular
     * stored run yet).
     */
    void initRead();

    /**
     * Begins reading a particular run.
     *
     * @param pStoredRunInit run to read
     */
    void startRead(
        SharedSegStreamAllocation pStoredRunInit);

    /**
     * Terminates read for the current run if any.
     */
    void resetRead();

    /**
     * Stores a run.
     *
     * @param subStream substream whose contents are to be fetched
     * and stored as a run
     */
    void storeRun(ExternalSortSubStream &subStream);

    /**
     * @return information about run created by storeRun()
     */
    SharedSegStreamAllocation getStoredRun();

    /**
     * Releases any resources acquired by this accessor.
     */
    void releaseResources();

    // implement ExternalSortSubStream
    virtual ExternalSortFetchArray &bindFetchArray();
    virtual ExternalSortRC fetch(uint nTuplesRequested);
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortRunAccessor.h
