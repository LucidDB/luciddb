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

#ifndef Fennel_LcsClusterNodeWriter_Included
#define Fennel_LcsClusterNodeWriter_Included

#include "fennel/lucidera/colstore/LcsClusterAccessBase.h"
#include "fennel/lucidera/colstore/LcsBitOps.h"
#include "fennel/lucidera/colstore/LcsClusterDump.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/tuple/TupleData.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

const int LcsMaxRollBack = 8;
const int LcsMaxLeftOver = 7;
const int LcsMaxSzLeftError = 4;

enum ForceMode { none = 0, fixed = 1, variable = 2 };
    
/**
 * Constructs a cluster page, managing the amount of space currently in use
 * on the page and determining the offsets where different elements are to
 * be stored
 */
class LcsClusterNodeWriter : public LcsClusterAccessBase, public TraceSource
{
private:
    /**
     * Writes btree corresponding to cluster
     */
    SharedBTreeWriter bTreeWriter;

    /**
     * Accessor for scrath segments
     */
    SegmentAccessor scratchAccessor;

    /**
     * Lock on scratch page
     */
    ClusterPageLock bufferLock;

    /**
     * Cluster page header
     */
    PLcsClusterNode m_pHdr;
    
    /**
     * Size of the cluster page header
     */
    uint m_pHdrSize;
    
    /**
     * Cluster page to be written
     */
    PBuffer m_indexBlock;
    
    /**
     * Array of pointers to temporary blocks, 1 block for each column cluster
     */
    PBuffer *m_pBlock;

    /**
     * Size of the cluster page
     */
    uint m_szBlock;

    /**
     * Minimum size left on the page
     */
    int m_rIMinSzLeft;

    /**
     * Batch directories for the batches currently being constructed,
     * one per cluster column
     */
    boost::scoped_array<LcsBatchDir> m_batch;

    /**
     * Temporary storage for values, used for fixed mode batches; one per
     * cluster column
     */
    boost::scoped_array<PBuffer> m_pValBank;

    /**
     * First offset in the bank for each column in the cluster
     * value bank
     */
    boost::scoped_array<uint16_t> m_oValBank;

    /**
     * Start of each cluster column in the value bank
     */
    boost::scoped_array<uint16_t> m_pValBankStart;

    /**
     * Offsets to the batch directories on the temporary pages,
     * one per cluster column
     */
    boost::scoped_array<uint16_t> m_batchOffset;

    /**
     * Count of the number of batches in the temporary pages, one per
     * cluster column
     */
    boost::scoped_array<uint> m_batchCount;

    /**
     * Number of bytes left on the page
     */
    int m_szLeft;

    /**
     * Number of bits required to store the value codes for each column
     * in the cluster, for the batches currently being constructed
     */
    boost::scoped_array<uint> m_nBits;

    /**
     * Number of values that will cause the next nBit change for the column
     * in the cluster
     */
    boost::scoped_array<uint> m_nextWidthChange;

    /**
     * Indicates whether temporary arrays have already been allocated
     */
    bool m_allocArrays;

    /**
     * Set when the mode of a batch should be forced to a particular value
     */
    boost::scoped_array<ForceMode> m_bForceMode;

    /**
     * Number of times force mode has been used for each cluster column
     */
    boost::scoped_array<uint> m_forceModeCount;

    /**
     * Max value size encountered thus far for each cluster column
     */
    boost::scoped_array<uint> m_maxValueSize;

    /**
     * Cluster dump
     */
    SharedLcsClusterDump clusterDump;

    /**
     * Associates an offset with an address, determining whether a value is
     * stored in the temporary block or the temporary value bank
     *
     * @param lastValOffset offset of the last value for this particular column
     *
     * @param pValBank buffer storing values in the value bank
     *
     * @oValBank offset of first value for column in the value bank
     *
     * @pBlock temporary block for column
     *
     * @f desired offset
     *
     * @return address corresponding to offset
     */
    PBuffer ValueSource(uint16_t lastValOffset, PBuffer pValBank,
                            uint16_t oValBank, PBuffer pBlock,
                            uint16_t f)
    {
        // if value not in back use 
        if (f < lastValOffset)
            return pValBank + f - oValBank;
        else 
            return pBlock + f;
    }

    /**
     * Moves all cluster data from cluster page to temporary storage
     *
     * @return number of rows currently on page
     */
    RecordNum MoveFromIndexToTemp(); 

    /**
     * Moves all cluster data from temporary storage to the actual
     * cluster page
     */
    void MoveFromTempToIndex(); 

    /**
     * Allocates temporary arrays used during cluster writes
     */
    void AllocArrays();

    /**
     * Rounds a 32-bit value to a boundary of 8
     *
     * @param val value to be rounded
     */
    inline uint32_t round8Boundary(uint32_t val)
    {
        return val & 0xfffffff8;
    }

    /**
     * Rounds a 32-bit value to a boundary of 8 if it is > 8
     *
     * @param val value to be rounded
     */
    inline uint32_t roundIf8Boundary(uint32_t val)
    {
        if (val > 8) {
            return round8Boundary(val);
        }
    }

public:
     explicit LcsClusterNodeWriter(BTreeDescriptor &treeDescriptorInit,
                                   SegmentAccessor &accessorInit,
                                   SharedTraceTarget pTraceTargetInit,
                                   std::string nameInit);

    /**
     * Destructor
     */
    ~LcsClusterNodeWriter();

    /**
     * Gets the last cluster page
     *
     * @param pBlock output param returning the cluster page
     *
     * @param firstRid output param returning first rid stored on cluster page
     *
     * @return true if cluster is non-empty
     */
    bool getLastClusterPageForWrite(PLcsClusterNode &pBlock, LcsRid &firstRid);

    /**
     * Allocates a new cluster page
     *
     * @param firstRid first rid to be stored on cluster page
     *
     * @return page allocated
     */
    PLcsClusterNode allocateClusterPage(LcsRid firstRid);

    /**
     * Initializes object with parameters relevant to the cluster page that
     * will be written
     *
     * @param nColumns number of columns in the cluster
     *
     * @param indexBlock pointer to the cluster page to be written
     *
     * @param pBlock array of pointers to temporary pages to be used while
     * writing this cluster page
     *
     * @param szBlock size of cluster page, reflecting max amount of space
     * available to write cluster data
     */
    void Init(uint nColumns, PBuffer indexBlock, PBuffer *pBlock, uint szBlock);

    void Close();

    /**
     * Prepares a cluster page as a new one
     *
     * @param startRID first RID on the page
     */
    void OpenNew(LcsRid startRID);

    /**
     * Prepares an existing cluster page for appending new data
     *
     * @param nValOffsets pointer to output array reflecting the number of
     * values currently in each column on this page
     *
     * @param lastValOffsets pointer to output array reflecting the offset of
     * the last value currently on the page for each cluster column
     *
     * @param nrows returns number of rows currently on page
     */
    void OpenAppend(uint *nValOffsets, uint16_t *lastValOffsets,
                    RecordNum &nrows);
    
    /**
     * Returns parameters describing the last batch for a given column
     *
     * @param column the column to be described
     *
     * @param dRow output parameter returning the number of rows over
     * the multiple of 8 boundary
     *
     * @param recSize output parameter returning the record size for the
     * batch
     */
    void DescribeLastBatch(uint column, uint &dRow, uint &recSize);
    
    /**
     * Returns the offset of the next value in a batch
     *
     * @param column column we want the value for
     *
     * @thisVal offset of the value currently positioned at
     *
     * @szVal number of bytes in the value referenced by "thisVal"
     *
     * @return offset of the value after "thisVal"
     */
    uint16_t GetNextVal(uint column, uint16_t thisVal);

    /**
     * Rolls back the last 8 value (or less) from a batch
     *
     * @param column column to be rolled back
     *
     * @param pVal buffer where the rolled back values will be copied; 
     * the buffer is assumed to be fixedRec * (nRows % 8) in size, as
     * determined by the last call to describeLastBatch
     */
    void RollBackLastBatch(uint column, PBuffer pVal);

    /**
     * Returns true if the batch is not being forced to compress mode
     *
     * @param column column being described
     */
    inline bool NoCompressMode(uint column) const { 
        return m_bForceMode[column] == fixed ||
                m_bForceMode[column] == variable;
    };

    /**
     * Translates an offset for a column to the pointer to the actual value
     *
     * @param column offset corresponds to this column
     *
     * @param offset offset to be translated
     *
     * @return pointer to value
     */
    inline PBuffer GetOffsetPtr(uint column, uint16_t offset)
        { return m_pBlock[column] + offset; };

    /**
     * Adds a value to the page, in the case where the value already exists
     * in the column
     *
     * @param column column corresponding to the value being added
     *
     * @param bFirstTimeInBatch true if this is the first time the value
     * is encountered for this batch
     *
     * @return true if there is enough room in the page for the value
     */
    bool AddValue(uint column, bool bFirstTimeInBatch);

    /**
     * Adds a new value to the page.  In the case of compressed or variable
     * mode, adds the value to the bottom of the page.  In the case of
     * fixed mode, adds the value to the "value bank".
     *
     * @param column column corresponding to the value being added
     *
     * @param pVal value to be added
     *
     * @param oVal returns the offset where the value has been added
     *
     * @return true if there is enough room in the page for the value
     */
    bool AddValue(uint column, PBuffer pVal, uint16_t *oVal);

    /**
     * Undoes the last value added to the current batch for a column
     *
     * @param column column corresponding to the value to be undone
     *
     * @param pVal value to be undone
     *
     * @param bFirstTimeInBatch true if the value being undone is the first
     * such value for the batch
     */
    void UndoValue(uint column, PBuffer pVal, bool bFirstInBatch);

    /**
     * Writes a compressed mode batch into the temporary cluster page for
     * a column.  Only a multiple of 8 rows is written, if this is not the
     * last batch in the cluster.
     *
     * Excess rows are written into a temporary buffer.  If this is the last
     * batch in the load, then it is ok to have < 8 rows, as the next load
     * will roll it back to fill it up with more rows.
     *
     * Note that it is assumed that the caller has already copied the
     * key offsets for this batch into the cluster page.  This call will
     * only copy the bit vectors and batch directory corresponding to this
     * batch
     *
     * @param column column corresponding to the batch
     *
     * @param pRows array mapping rows to key offsets
     *
     * @param pBuf temporary buffer where excess row values will be copied;
     * assumed to be (nRow % 8)*fixedRec big
     */
    void PutCompressedBatch(uint column, PBuffer pRows, PBuffer pBuf);

    /**
     * Writes a fixed or variable mode batch into a temporary cluster page for
     * a column.  Only a multiple of 8 rows is written, if this is not the
     * last batch in the cluster.
     *
     * Excess rows are written into a temporary buffer.  If this is the last
     * batch in the load, then it is ok to have < 8 rows, as the next load
     * will roll it back to fill it up with more rows.
     *
     * In the variable mode case, the key offsets are written to the batch
     * area on the page.  In the fixed mode case, the values themselves are
     * written to the batch area.  In both cases, the batch directory is
     * also written out.
     *
     * @param column column corresponding to the batch
     *
     * @param pRows array of offsets to values
     *
     * @param pBuf temporary buffer where excess row values will be copied;
     * assumed to be (nRow % 8)*fixedRec big
     */
    void PutFixedVarBatch(uint column, uint16_t *pRows, PBuffer pBuf);

    /**
     * Determines which compression mode to use for a batch
     *
     * @param column column for which compression mode is being determined
     *
     * @param fixedSize size of record in the case of fixed size compression
     *
     * @param nRows number of rows in the batch
     *
     * @param pValOffset returns a pointer to the offset of the start of
     * the batch
     *
     * @param compressionMode returns the chosen compression mode
     */
    void PickCompressionMode(uint column, uint fixedSize, uint nRows,
                                uint16_t **pValOffset,
                                LcsBatchMode &compressionMode);

    /**
     * Returns true if there is no space left in the cluster page
     */
    bool IsEndOfBlock() { 
        uint col;
        int valueSizeNeeded;
        
        for (valueSizeNeeded = 0, col = 0; col < nClusterCols; col++)
            valueSizeNeeded += m_batch[col].recSize * LcsMaxLeftOver;

        return m_szLeft <= (m_rIMinSzLeft + valueSizeNeeded); 
    }

    /**
     * Done with the current cluster page.  Moves all data from temporary
     * pages into the real cluster page
     */
    void EndBlock() { MoveFromTempToIndex(); }
};

FENNEL_END_NAMESPACE

#endif

// End LcsClusterNode.h
