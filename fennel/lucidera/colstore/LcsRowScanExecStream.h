/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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

#include <boost/scoped_array.hpp>
#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/lucidera/colstore/LcsRowScanBaseExecStream.h"
#include "fennel/lucidera/bitmap/LbmRidReader.h"
#include "fennel/lucidera/colstore/LcsResidualColumnFilters.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Parameters specific to the row scan execution stream, including the type
 * of scan (full table scan versus specific rid reads) and whether residual
 * filtering should be performed.
 */
struct LcsRowScanExecStreamParams : public LcsRowScanBaseExecStreamParams
{
    /**
     * If true, this scan performs a full table scan.  In that case, the
     * first input into the stream will be those rids that are to be excluded
     * from the scan.  Otherwise, if this is false, the first input to the
     * stream contains the list of rids that the stream should read.
     */
    bool isFullScan;

    /**
     * If true, this ExecStream contains extra residual filters that should
     * be applied during the scan.  If n columns contain filters, then those
     * filters are contained in input streams 1 through n, where each stream
     * contains only those filters specific to each column.
     */
    bool hasExtraFilter;

    /**
     * contains an array of column id corresponding to each filter column
     */
    TupleProjection residualFilterCols;
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
     * Tuple data for all columns read from all clusters, including    
     * filter columns
     */
    TupleDataWithBuffer outputTupleData;

    /**
     * This variable is used to control the initialization 
     * of residual filters.  It's 1 less than the index of 
     * the first filtering input to read.  After open, it's 
     * initializaed to 0.  On execute, the filtering inputs
     * are read sequentially, while this variable is incremented,
     * until an underflow or all filtering inputs have been read. 
     * On return due to an underflow, this variable allows reading to
     * resume where it had left off.
     */
    uint iFilterToInitialize;

    /*
     * Real output tuple.
     */
    TupleData projOutputTupleData;

    /*
     * projection for the output row.
     */
    TupleProjection outputProj;

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

    /**
     * The local filter data structure.
     * Note that these are aliasing pointers
     * to facilitate filter data initialization
     * and memory deallocation.
     */
    boost::scoped_array<LcsResidualColumnFilters *> filters;

    /**
     * Builds outputProj from params.
     *
     * @param outputProj the projection to be built
     *
     * @param params the LcsRowScanBaseExecStreamParams
     *
     */
    virtual void buildOutputProj(TupleProjection &outputProj,
                                 LcsRowScanBaseExecStreamParams const &params);

    /**
     * initializes the filter data structures
     *
     * @return false iff input under flows.
     */
    bool initializeFiltersIfNeeded();

    /**
     * initializes the filter data structures during prepare time
     *
     * @param params the LcsRowScanExecStreamParams
     */
    void prepareResidualFilters(LcsRowScanExecStreamParams const &params);

    /**
     * Clears data structures used in residual filtering
     */
    void clearFilterData();

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
