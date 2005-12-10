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

#include "fennel/lucidera/colstore/LcsClusterReader.h"
#include "fennel/exec/ConduitExecStream.h"
#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleProjectionAccessor.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Represents a single cluster in a table cluster scan
 */
struct LcsClusterScanDef : public BTreeExecStreamParams
{
    /**
     * Tuple descriptor of columns that make up the cluster
     */
    TupleDescriptor clusterTupleDesc;
};

typedef std::vector<LcsClusterScanDef> LcsClusterScanDefList;

/**
 * Indicates the clustered indexes that need to be read to scan a table and
 * the columns from the clusters that need to be projected in the scan result.
 */
struct LcsRowScanExecStreamParams : public ConduitExecStreamParams
{
    /**
     * Ordered list of cluster scans
     */
    LcsClusterScanDefList lcsClusterScanDefs;

    /**
     * projection from scan
     */
    TupleProjection outputProj;
};

/**
 * Given a stream of RIDs, performs a table scan for those RIDs using
 * the appropriate clustered indexes defined on the table. The stream
 * returns a projected subset of columns from the table
 */
class LcsRowScanExecStream : public ConduitExecStream
{
    /**
     * TupleData for tuple representing incoming stream of RIDs
     */
    TupleData inputTuple;

    /**
     * Tuple descriptor representing the full table row
     */
    TupleDescriptor rowTupleDesc;

    /**
     * Tuple data representing the full table row
     */
    TupleData rowTupleData;

    /**
     * Tuple accessor for full table row
     */
    TupleAccessor rowTupleAccessor;

    /**
     * Buffer for full table row
     */
    boost::scoped_array<FixedBuffer> rowTupleBuffer;

    /**
     * Number of clusters to be scanned
     */
    uint nClusters;

    /**
     * Array containing info about clusters
     */
    boost::scoped_array<SharedLcsClusterReader> pClusters;

    /**
     * Tuple data for input stream
     */
    TupleData ridTupleData;
    
    /**
     * Tuple accessor for projected row
     */
    TupleProjectionAccessor projAccessor;

    /**
     * Descriptor for projected tuple
     */
    TupleDescriptor projDescriptor;

    /**
     * Tuple data for projected output tuple
     */
    TupleData projData;

    /**
     * Number of rids read
     */
    RecordNum nRidsRead;

    /**
     * Current rid being fetched
     */
    LcsRid rid;

    /**
     * true if tuple has been read and not yet produced
     */
    bool tupleFound;

    /**
     * true if executing full table scan
     */
    bool fullTableScan;

    /** 
     * true if produceTuple pending
     */
    bool producePending;

#ifdef NOT_DONE_YET
    BBRC Start();
    // implement RIScan::RidPrefetchSource
    RID PrefetchNextRid(RID ridMin = RID_MIN);
    // fetch a buffered RID (and release its slot if bCount)
    RID FetchNextRid(RID ridMin, bool& bDeleted, BOOL bCount);
    // fill m_prefetchBuf with RID's less than EndRid
    BBRC FillPrefetchBuffer(RID EndRid);
    // called when the next deleted row may have been skipped
    BBRC SkipDeletedRows();
#endif /* NOT_DONE_YET */

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
