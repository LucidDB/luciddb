/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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

#ifndef Fennel_LcsClusterDump_Included
#define Fennel_LcsClusterDump_Included

#include "fennel/btree/BTreeDescriptor.h"
#include "fennel/common/TraceSource.h"
#include "fennel/lucidera/colstore/LcsClusterAccessBase.h"
#include <boost/enable_shared_from_this.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Class used to dump the contents of a cluster page using fennel trace
 */
class LcsClusterDump : public LcsClusterAccessBase, public TraceSource
{
    /**
     * The level at which tracing of cluster dump will be done.
     * I.e., the caller of this object can control the level at
     * which dumps are generated.
     */
    TraceLevel traceLevel;

    /**
     * Tuple descriptor for the columns in a cluster page
     */
    TupleDescriptor colTupleDesc;

    void callTrace(char *format, ...);

    PBuffer fprintVal(uint idx, PBuffer pV, uint col);

public:
    explicit LcsClusterDump(
        BTreeDescriptor const &bTreeDescriptor,
        TupleDescriptor const &colTupleDescInit,
        TraceLevel traceLevelInit,
        SharedTraceTarget pTraceTarget,
        std::string name);

    /**
     * Dumps out a cluster page
     *
     * @param pageId pageid of the cluster page
     *
     * @param pHdr pointer to the cluster page
     *
     * @param szBlock number of bytes in the page
     */
    void dump(uint64_t pageId, PConstLcsClusterNode pHdr, uint szBlock);
};


FENNEL_END_NAMESPACE

#endif

// End LcsClusterDump.h
