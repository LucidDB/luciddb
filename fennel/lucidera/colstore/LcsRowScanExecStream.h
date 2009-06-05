/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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
#include <boost/scoped_ptr.hpp>
#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/lucidera/colstore/LcsRowScanBaseExecStream.h"
#include "fennel/lucidera/bitmap/LbmRidReader.h"
#include "fennel/lucidera/colstore/LcsResidualColumnFilters.h"
#include "fennel/common/BernoulliRng.h"
#include "fennel/common/FemEnums.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Parameters specific to the row scan execution stream, including the type
 * of scan (full table scan versus specific rid reads) and whether residual
 * filtering should be performed.
 */
struct FENNEL_LCS_EXPORT LcsRowScanExecStreamParams
    : public LcsRowScanBaseExecStreamParams
{
    static int32_t defaultSystemSamplingClumps;

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

    /**
     * The configured sampling mode for this row scan: off, Bernoulli or
     * system.
     */
    TableSamplingMode samplingMode;

    /**
     * Percentage of rows to return as a sample.  Expressed as a fraction
     * between 0.0 and 1.0.
     */
    float samplingRate;

    /**
     * Flag indicating whether sample results should be repeatable (assuming no
     * changes to the structure or contents of the table.
     */
    bool samplingIsRepeatable;

    /**
     * Seed value for random number generators to be used for repeatable
     * sampling.
     */
    int32_t samplingRepeatableSeed;

    /**
     * Number of sample clumps to produce during system-mode sampling.
     */
    int32_t samplingClumps;

    /**
     * Number of rows in the table, to support system-mode sampling.  This
     * field is NOT a count of the number of rows in the expected sample.  The
     * term "sampling" in the field's name refers to its use as a parameter
     * specific to sampling.
     */
    int64_t samplingRowCount;
};

/**
 * Given a stream of RIDs, performs a table scan for those RIDs using
 * the appropriate clustered indexes defined on the table. The stream
 * returns a projected subset of columns from the table
 */
class FENNEL_LCS_EXPORT LcsRowScanExecStream
    : public LcsRowScanBaseExecStream
{
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
     * Current rid read from the input stream
     */
    LcsRid inputRid;

    /**
     * Next rid that needs to be fetched
     */
    LcsRid nextRid;

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
     * The number of residual column filters configured.
     */
    int32_t nFilters;

    /**
     * One of SAMPLING_OFF, SAMPLING_BERNOULLI or SAMPLING_SYSTEM.
     */
    TableSamplingMode samplingMode;

    /**
     * the sampling rate (0.0 to 1.0)
     */
    float samplingRate;

    /**
     * true if the sample should be repeatable
     */
    bool isSamplingRepeatable;

    /**
     * seed for repeatable sampling
     */
    int32_t repeatableSeed;

    /**
     * number of clumps for system sampling
     */
    int32_t samplingClumps;

    /**
     * size of each sampling clump
     */
    uint64_t clumpSize;

    /**
     * distance (in rows) between each clump
     */
    uint64_t clumpDistance;

    /**
     * position (0 to clumpSize) in current clump
     */
    uint64_t clumpPos;

    /**
     * position (clumpDistance to 0) in between clumps
     */
    uint64_t clumpSkipPos;

    /**
     * The number of clumps that need to be built
     */
    uint numClumps;

    /**
     * Running counter of the number of clumps built
     */
    uint numClumpsBuilt;

    /**
     * RNG for Bernoulli sampling.
     */
    boost::scoped_ptr<BernoulliRng> samplingRng;

    /**
     * Number of rows in the table.  Used only for sampling.  In the
     * case of Bernoulli sampling, includes count of deleted rows.
     */
    int64_t rowCount;

    /**
     * True if completed building rid runs
     */
    bool ridRunsBuilt;

    /**
     * Current rid run being constructed
     */
    LcsRidRun currRidRun;

    /**
     * Iterator over the circular buffer containing rid runs
     */
    CircularBufferIter<LcsRidRun> ridRunIter;

    /**
     * Builds outputProj from params.
     *
     * @param outputProj the projection to be built
     *
     * @param params the LcsRowScanBaseExecStreamParams
     *
     */
    virtual void buildOutputProj(
        TupleProjection &outputProj,
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
     * Initializes the system sampling data structures during open time.
     */
    void initializeSystemSampling();

    /**
     * Populates the circular rid run buffer.
     *
     * @return EXECRC_YIELD if buffer successfully populated
     */
    ExecStreamResult fillRidRunBuffer();

public:
    LcsRowScanExecStream();
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
