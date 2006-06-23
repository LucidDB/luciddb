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

#ifndef Fennel_LcsRowScanExecStream_Included
#define Fennel_LcsRowScanExecStream_Included

#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/lucidera/colstore/LcsRowScanBaseExecStream.h"
#include "fennel/lucidera/bitmap/LbmRidReader.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Indicates the clustered indexes that need to be read to scan a table and
 * the columns from the clusters that need to be projected in the scan result.
 */
struct LcsRowScanExecStreamParams : public LcsRowScanBaseExecStreamParams
{
    /**
     * Does this ExecStream perform full scan.
     */
    bool isFullScan;

    /**
     * Does this ExecStream contain extra filter(as a range list input).
     */
    bool hasExtraFilter;
};

/**
 * Given a stream of RIDs, performs a table scan for those RIDs using
 * the appropriate clustered indexes defined on the table. The stream
 * returns a projected subset of columns from the table
 */
class LcsRowScanExecStream : public LcsRowScanBaseExecStream
{
    /**
     * TupleData for tuple representing incoming stream of RIDs
     */
    TupleData inputTuple;

    /**
     * Tuple data for projected columns read from all clusters, in projection
     * order
     */
    TupleDataWithBuffer outputTupleData;

    /**
     * Tuple data for input stream
     */
    TupleData ridTupleData;
    
    /**
     * Rid reader
     */
    LbmRidReader ridReader;

    /**
     * Number of rids read
     */
    RecordNum nRidsRead;

    /**
     * Current rid being fetched
     */
    LcsRid rid;

    /**
     * True if need to read a new deleted rid from the input stream
     */
    bool readDeletedRid;

    /**
     * True if reached EOS on deleted rid input stream
     */
    bool deletedRidEos;

    /**
     * Current deleted rid
     */
    LcsRid deletedRid;

    /**
     * true if tuple has been read and not yet produced
     */
    bool tupleFound;

    /**
     * true if executing full table scan
     */
    bool isFullScan;

    /**
     * true if there's extra range list filter(as the last input)
     */
    bool hasExtraFilter;

    /** 
     * true if produceTuple pending
     */
    bool producePending;

public:
    virtual void prepare(LcsRowScanExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End LcsRowScanExecStream.h
