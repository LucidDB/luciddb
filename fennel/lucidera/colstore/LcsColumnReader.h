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

#ifndef Fennel_LcsColumnReader_Included
#define Fennel_LcsColumnReader_Included

#include "fennel/lucidera/colstore/LcsClusterNode.h"
#include "fennel/lucidera/colstore/LcsBitOps.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Reads RIDs from a single cluster
 */
class LcsColumnReader
{
    /**
     * Parent cluster reader object
     */
    LcsClusterReader *pScan;

    /**
     * Ordinal of column in cluster (0-based)
     */
    uint colOrd;

    /**
     * Batch corresponding to column
     */
    PLcsBatchDir pBatch;

    /**
     * Values of the batch
     */
    PBuffer pValues;

    /**
     * Base address from which offsets are stored
     */
    PBuffer pBase;

    /**
     * Pointers to the origin of each bit vector
     */
    PtrVec origin;

    /**
     * Width of bit vectors
     */
    WidthVec width;
    
    /* Number of entries per vector
     */
    uint iV;
    
    /**
     * Pointer to appropriate bit vector read function
     */
    PBitVecFuncPtr pFuncReadBitVec;

    // REVIEW jvs 27-Dec-2005:  Either someone at Broadbase didn't
    // know how to use pointer-to-member-functions, or Visual C++ had
    // a bug.  Anyway, should be able to use them instead of statics.

    /**
     * Retrieves the current value of the column from a fixed mode batch
     */
    const PBuffer (*pGetCurrentValueFunc)(const LcsColumnReader* me);

    /**
     * Returns value from compressed batch
     */
    static const PBuffer getCompressedValue(const LcsColumnReader* me);

    /**
     * Returns  value from fixed batch
     */
    static const PBuffer getFixedValue(const LcsColumnReader* me);

    /**
     * Returns value from variable batch
     */
    static const PBuffer getVariableValue(const LcsColumnReader* me);

public:
    /**
     * Initializes a scan of column "colOrd"
     *
     * @param colOrd column number within cluster; 0-based
     */
    void init(LcsClusterReader *pScanInit, uint colOrdInit)
    {
        pScan = pScanInit;
        colOrd = colOrdInit;
    }

    /**
     * Synchronizes batches for each column when the scan moves to a new
     * range
     */
    void sync();

    /**
     * Returns true if current batch for column is compressed
     */
    bool batchIsCompressed() const
    { 
        return pBatch->mode == LCS_COMPRESSED;
    }

    /**
     * Returns true if current batch for column is fixed
     */
    bool batchIsFixed() const
    { 
        return pBatch->mode == LCS_FIXED;
    }

    /**
     * Returns current value code for a compressed batch entry
     */
    const PBuffer getCurrentValue() const
    {
        return pGetCurrentValueFunc(this);
    }

    /**
     * Gets the code (between 0 and GetBatchValCount() - 1) of the current
     * value for a compressed batch
     */
    uint16_t getCurrentValueCode() const;

    // The following functions access the distinct values in a compressed
    // batch.  The offsets table contains one offset for each distinct value.
    // GetBatchValue() finds the address of i-th distinct value by adding the
    // base pointer of the batch to the i-th entry in the offset table.
    /**
     * Returns number of distinct values in batch
     */
    uint getBatchValCount() const
    { 
        return pBatch->nVal;
    }

    /**
     * Returns base pointer of batch
     */
    const PBuffer getBatchBase() const
    { 
        return pBase;
    }

    /**
     * Returns table of offsets from base
     */
    const uint16_t *getBatchOffsets() const
    { 
        return (const uint16_t *) pValues;
    }

    /** 
     * Gets iValCode-th value
     *
     * @param iValCode code corresponding to value to be retrieved from batch
     */
    const PBuffer getBatchValue(uint iValCode) const
    {
        return (const PBuffer) (getBatchBase() + getBatchOffsets()[iValCode]);
    }

    /**
     * Reads up to "count" value-codes into the "pValCodes" table.  If we reach
     * the end of the batch, "*pActCount", the number of value-codes actually
     * read, is less than "count".
     *
     * For example, if we are at rid 1000, then "pValCodes[i]" will hold the
     * code for the value of the indexed column at rid 1000 + i.  To convert
     * this code to a value, use "GetBatchValue(pValCodes[i])".
     *
     * Note: This method may be used only with compressed batches.  
     *
     * @param count how many value codes to read
     *
     * @param pValCodes output param for table for "count" value codes
     *
     * @param pActCount output param for actual number of value codes returned
     */
    void readCompressedBatch(uint count, uint16_t *pValCodes, uint *pActCount);
};

FENNEL_END_NAMESPACE

#endif

// End of LcsColumnReader.h
