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

#ifndef Fennel_LbmExecStreamTestBase_Included
#define Fennel_LbmExecStreamTestBase_Included

#include "fennel/test/ExecStreamUnitTestBase.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/exec/ValuesExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/lucidera/bitmap/LbmEntry.h"
#include "fennel/lucidera/bitmap/LbmNormalizerExecStream.h"
#include "fennel/lucidera/sorter/ExternalSortExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Structure for passing input data corresponding to bitmap inputs and
 * expected result
 */
struct InputData
{
    /**
     * Number of bytes in each bitmap segment
     */
    uint bitmapSize; 

    /**
     * Initial rid value represented in the bitmap
     */
    LcsRid startRid;

    /**
     * Number of rids to skip in between each rid
     */
    uint skipRows;
};

/**
 * Structure containing information about the constructed bitmaps corresponding
 * the inputs and expected result
 */
struct BitmapInput
{
    /**
     * Buffers storing the bitmap segments
     */
    boost::shared_array<FixedBuffer> bufArray;
    
    /**
     * Amount of space currently used in buffer
     */
    uint currBufSize;

    /**
     * Size of the buffer
     */
    uint fullBufSize;

    /**
     * Number of bitmap segments
     */
    uint nBitmaps;
};

class NumberStream;
typedef boost::shared_ptr<NumberStream> SharedNumberStream;

/**
 * Interface for defining a stream of numbers. The interface is generic
 * for defining various kinds of streams: union, intersect, fibonnaci, etc.
 */
class NumberStream
{
public:
    virtual ~NumberStream() 
    {}

    // invalid/null value
    static const uint BIG_NUMBER = 0xffffffff;

    // clones this object
    virtual NumberStream *clone() = 0;

    // upper row count limit, used to allocate data buffer
    virtual uint getMaxRowCount(uint maxRid) = 0;

    // whether the stream has any more numbers
    virtual bool hasNext() = 0;

    // returns the next number
    virtual uint getNext() = 0;
};

/**
 * From (first .. last), increments of skip
 */
class SkipNumberStream : public NumberStream
{
    uint first, last, skip;
    uint prev, next;

public:
    SkipNumberStream(uint first, uint last, uint skip)
    {
        this->first = first;
        this->last = last;
        this->skip = skip;
        next = first;
    }

    NumberStream *clone() 
    {
        return new SkipNumberStream(first, last, skip);
    }

    uint getMaxRowCount(uint maxRid)
    {
        uint upperBound = std::min(maxRid, last);
        return ((upperBound - first) / skip) + 1;
    }

    bool hasNext() 
    {
        return next <= last;
    }

    uint getNext() 
    {
        uint value = next;
        next += skip;
        return value;
    }
};

/**
 * Unions multiple number streams from 1 ... BIG_NUMBER-1
 */
class UnionNumberStream : public NumberStream
{
    std::vector<SharedNumberStream> children;
    std::vector<uint> currentValues;
    uint prev, next;

    bool findNext() 
    {
        if (next != BIG_NUMBER) {
            return true;
        }
        // scan to values greater than prev
        for (uint i = 0; i < children.size(); i++) {
            if (currentValues[i] <= prev) {
                if (children[i]->hasNext()) {
                    currentValues[i] = children[i]->getNext();
                }
            }
        }
        // search for lowest value greater than prev
        for (uint i = 0; i < children.size(); i++) {
            if (currentValues[i] > prev && currentValues[i] < next) {
                next = currentValues[i];
            }
        }
        return next != BIG_NUMBER;
    }

public:
    UnionNumberStream()
    {
        prev = 0;
        next = BIG_NUMBER;
    }

    virtual ~UnionNumberStream() 
    {}

    void addChild(SharedNumberStream pStream) 
    {
        children.push_back(pStream);
        currentValues.push_back(0);
    }

    NumberStream *clone() 
    {
        UnionNumberStream *pStream = new UnionNumberStream();
        for (uint i = 0; i < children.size(); i++) {
            SharedNumberStream pChild(children[i]->clone());
            pStream->addChild(pChild);
        }
        return pStream;
    }

    uint getMaxRowCount(uint maxRid)
    {
        uint total = 0;
        for (uint i = 0; i < children.size(); i++) {
            total += children[i]->getMaxRowCount(maxRid);
        }
        return total;
    }

    bool hasNext() 
    {
        return findNext();
    }

    uint getNext()
    {
        bool hasNext = findNext();
        assert(hasNext);
        prev = next;
        next = BIG_NUMBER;
        return prev;
    }
};

/**
 * Combines a NumberStream with other attributes used to make test data
 */
struct LbmNumberStreamInput
{
    SharedNumberStream pStream;
    uint bitmapSize; 
};

class NumberStreamExecStreamGenerator : public MockProducerExecStreamGenerator
{
protected:
    SharedNumberStream pStream;
public:
    NumberStreamExecStreamGenerator(SharedNumberStream pNumberStream)
    {
        pStream = SharedNumberStream(pNumberStream->clone());
    }

    virtual int64_t generateValue(uint iRow, uint iCol)
    {
        if (pStream->hasNext()) {
            return pStream->getNext();
        }
        return 0;
    }
};

static const std::string traceName = "net.sf.fennel.test.lbm";

/**
 * LbmExecStreamTestBase is the base class for tests of bitmap exec streams.
 *
 * @version $Id$
 */
class LbmExecStreamTestBase : public ExecStreamUnitTestBase
{
protected:
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc_int64;
    TupleAttributeDescriptor attrDesc_bitmap;

    /**
     * Size of bitmap columns
     */
    uint bitmapColSize;
    
    /**
     * Tuple descriptor, tupledata, and accessor for a bitmap segment:
     * (rid, segment descriptor, bitmap segments)
     */
    TupleDescriptor bitmapTupleDesc;
    TupleData bitmapTupleData;
    TupleAccessor bitmapTupleAccessor;

    /**
     * Tuple descriptor, data, and accessor for key-containting bitmaps
     * (keys, srid, segment descriptor, bitmap segments)
     */
    TupleDescriptor keyBitmapTupleDesc;
    TupleData keyBitmapTupleData;
    TupleAccessor keyBitmapTupleAccessor;
    boost::shared_array<FixedBuffer> keyBitmapBuf;
    uint keyBitmapBufSize;

    inline static const std::string &getTraceName() 
    {
        return traceName;
    }

    // common, and specific functions for initializing inputs
    void initBitmapInput(
        BitmapInput &bmInput, uint nRows, InputData const &inputData);
    void initBitmapInput(
        BitmapInput &bmInput, uint nRows, LbmNumberStreamInput input);

    void generateBitmaps(
        uint nRows, LbmNumberStreamInput input, BitmapInput &bmInput);

    void produceEntry(
        LbmEntry &lbmEntry, TupleAccessor &bitmapTupleAccessor,
        BitmapInput &bmInput);

    void initValuesExecStream(
        uint idx, ValuesExecStreamParams &valuesParams,
        ExecStreamEmbryo &valuesStreamEmbryo, BitmapInput &bmInput);

    void initSorterExecStream(
        ExternalSortExecStreamParams &params,
        ExecStreamEmbryo &embryo,
        TupleDescriptor const &outputDesc, 
        uint nKeys = 1);

    void initNormalizerExecStream(
        LbmNormalizerExecStreamParams &params,
        ExecStreamEmbryo &embryo, 
        uint nKeys);

    /**
     * Calculates size of result bitmap.
     *
     * @param start start of result range, inclusive
     * @param end end of result range, exclusive
     */
    uint resultBitmapSize(uint start, uint end)
    {
        return resultBitmapSize(end - start);
    }

    uint resultBitmapSize(uint nRids)
    {
        // the result bitmap should be large enough for all rids in range,
        // nRids/8, plus extra space that allows the segment builder some
        // breathing room to operate
        uint extraSpace = 16;
        return (nRids / 8) + extraSpace;
    }

    /**
     * Generate bitmaps to used in verifying result of bitmap index scan
     *
     * @param nRows number of rows in index
     *
     * @param start initial rid value
     *
     * @param skipRows generate rids every "skipRows" rows; i.e., if skipRows
     * == 1, there are no gaps in the rids
     *
     * @param pBuf buffer where bitmap segment tuples will be marshalled
     *
     * @param bufSize amount of space currently used within pBuf
     *
     * @param fullBufSize size of pBuf
     *
     * @param nBitmaps returns number of bitmaps generated
     *
     * @param includeKeys if true, include the keys in the generated bitmap
     * entry
     */
    void generateBitmaps(
        uint nRows, uint start, uint skipRows, PBuffer pBuf, uint &bufSize,
        uint fullBufSize, uint &nBitmaps, bool includeKeys = false);

    void produceEntry(
        LbmEntry &lbmEntry, TupleAccessor &bitmapTupleAccessor, PBuffer pBuf,
        uint &bufSize, uint &nBitmaps, bool includeKeys);

    /**
     * Initialize bitmaps with keys
     */
    void initKeyBitmap(uint nRows, std::vector<int> const &repeatSeqValues);

public:
    void testCaseSetUp();

    /**
     * Find the interval for which an entire tuple's sequence repeats
     */
    static uint getTupleInterval(
        std::vector<int> const &repeatSeqValues,
        uint nKeys = 0);
};

FENNEL_END_NAMESPACE

#endif

// End LbmExecStreamTestBase.h
