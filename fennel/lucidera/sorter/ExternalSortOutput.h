/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
// Portions Copyright (C) 2004-2005 John V. Sichi
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

#ifndef Fennel_ExternalSortOutput_Included
#define Fennel_ExternalSortOutput_Included

#include "fennel/tuple/TupleAccessor.h"
#include "fennel/lucidera/sorter/ExternalSortSubStream.h"
#include "fennel/exec/ExecStreamDefs.h"

FENNEL_BEGIN_NAMESPACE

class ExternalSortInfo;

/**
 * ExternalSortMerger marshals XO output buffers by fetching from a
 * top-level ExternalSortSubStream.
 */
class ExternalSortOutput
{
    /**
     * Global information.
     */
    ExternalSortInfo &sortInfo;

    // TODO:  comment or replace
    TupleAccessor tupleAccessor;

    /**
     * Substream from which to fetch.
     */
    ExternalSortSubStream *pSubStream;

    /**
     * Fetch array bound to substream.
     */
    ExternalSortFetchArray *pFetchArray;

    /**
     * 0-based index of next tuple to return from fetch array.
     */
    uint iCurrentTuple;

public:
    explicit ExternalSortOutput(ExternalSortInfo &info);
    virtual ~ExternalSortOutput();

    /**
     * Sets the substream from which to fetch.
     *
     * @param subStream new source
     */
    void setSubStream(ExternalSortSubStream &subStream);

    // TODO jvs 10-Nov-2004:  eliminate this overload
    /**
     * Fetches tuples and writes them to a result stream.
     *
     * @param resultOutputStream receives marshalled tuple data
     *
     * @return result (EXTSORT_SUCCESS or EXTSORT_ENDOFDATA)
     */
    ExternalSortRC fetch(ByteOutputStream &resultOutputStream);
    
    /**
     * Fetches tuples and writes them to a buffer.
     *
     * @param bufAccessor receives marshalled tuple data
     *
     * @return result
     */
    ExecStreamResult fetch(ExecStreamBufAccessor &bufAccessor);
    
    /**
     * Releases any resources acquired by this object.
     */
    void releaseResources();
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortOutput.h
