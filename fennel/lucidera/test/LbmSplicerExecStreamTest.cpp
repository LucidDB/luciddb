/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2007-2009 LucidEra, Inc.
// Copyright (C) 2007-2009 The Eigenbase Project
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/ExecStreamUnitTestBase.h"
#include "fennel/lucidera/bitmap/LbmSplicerExecStream.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ValuesExecStream.h"
#include "fennel/cache/Cache.h"
#include <stdarg.h>
#include <hash_set>

#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * Testcase for splicer exec stream
 */
class LbmSplicerExecStreamTest : public ExecStreamUnitTestBase
{
protected:
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc_int64;
    TupleAttributeDescriptor attrDesc_bitmap;

    /**
     * Splice multiple sets of random rids into an empty btree.  Each rid
     * is unique, and the rids within each set are in sort order.
     *
     * @param numSets number of sets
     * @param numRidsPerSet number of rids per set
     */
    void testSpliceRids(uint numSets, uint numRidsPerSet);

    /**
     * Generates multiple sets of rids, each with an equal number of sorted
     * rids.  The range of rids generated sequences from 0 to N.
     *
     * @param ridSets the sets of rids generated
     * @param numSets number of sets to generate
     * @param numRidsPerSet number of rids per set
     */
    void generateSeqRidSets(
        std::vector<std::vector<uint64_t> > &ridSets,
        uint numSets,
        uint numRidsPerSet);

    /**
     * Generates a buffer containing the input rids to be passed into the
     * splicer
     *
     * @param rids rids that will populate the buffer
     * @param ridBuffer the buffer to be populated
     * @param bufferSize size of the buffer after it has been populated
     */
    void generateRidInput(
        std::vector<uint64_t> const &rids,
        PBuffer ridBuffer,
        uint &bufferSize);

    /**
     * Splice tuples into an empty btree.  The rids represented in the tuples
     * are unique, random rid between 0 and "numRows" * "factor".  The tuples
     * are randomly represented as either singletons or bitmaps with three
     * rids set.  Each key value within the tuple is the same.
     *
     * @param numRows number of rows to be inserted
     * @param factor factor that determines the spread of random rid values
     * generated
     * @param nKeys number of keys in the index, excluding the startRid
     * @param multipleSplices if true, splice one tuple at a time rather than
     * passing multiple tuples into a single invocation of the splicer exec
     * stream
     */
    void testSpliceWithKeys(
        uint numRows,
        uint factor,
        uint nKeys,
        bool multipleSplices);

    /**
     * Generate N unique, sorted, random rids between 0 and nRids*factor
     *
     * @param rids rids generated
     * @param nRids number of rids to be generated
     * @param factor factor that determines the spread of random rid values
     * generated
     */
    void generateRandomRids(
        std::vector<uint64_t> &rids,
        uint nRids,
        uint factor);

    /**
     * Generates a buffer containing one or more tuples to be passed into the
     * splicer.  Each tuple is either a singleton or a bitmap with 3 rids.
     * The method randomly picks between the two.
     *
     * @param rids rids that will be represented in the tuples generated
     * @param currRidIdx index into the the rids vector corresponding to
     * the next rid to be inserted into the tuple
     * @param buffer the buffer to be populated
     * @param bufferSize size of the buffer after it has been populated
     * @param nKeys number of keys in the index to be spliced
     * @param oneTuple if true, generate only a single tuple
     */
    void generateTupleInput(
        std::vector<uint64_t> const &rids,
        uint &currRidIdx,
        PBuffer buffer,
        uint &bufferSize,
        uint nKeys,
        bool oneTuple);

    /**
     * Splices a buffer of input into a btree, which may already have bitmap
     * entries in it
     *
     * @param inputBuffer buffer containing input into splicer
     * @param inputBufSize size of the input buffer
     * @param numRows number of rows to be spliced
     * @param bTreeDesc descriptor of the btree being spliced into
     */
    void spliceInput(
        boost::shared_array<FixedBuffer> &inputBuffer,
        uint inputBufSize,
        uint numRows,
        BTreeDescriptor const &bTreeDesc);

    /**
     * Initializes BTreeParams structure
     *
     * @param param BTreeParams structure being initialized
     * @param bTreeDesc BTreeDescriptor used to initialize the structure
     */
    void initBTreeParam(BTreeParams &param, BTreeDescriptor const &bTreeDesc);

    /**
     * Initializes a BTreeDescriptor corresponding to a bitmap index, and
     * creates the btree
     *
     * @param bTreeDesc BTreeDescriptor to be initialized
     * @param nKeys number of keys in the bitmap index; excludes the startRid
     */
    void createBTree(BTreeDescriptor &bTreeDesc, uint nKeys);

    /**
     * Initializes a tuple descriptor corresponding to a bitmap index
     *
     * @param tupleDesc tupleDescriptor to be initialized
     * @param nKeys number ofkeys in the index; excludes the startRid
     */
    void initBTreeTupleDesc(TupleDescriptor &tupleDesc, uint nKeys);

public:
    explicit LbmSplicerExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(LbmSplicerExecStreamTest, testSpliceRids50);
        FENNEL_UNIT_TEST_CASE(
            LbmSplicerExecStreamTest, testSpliceRidsLargeSets);
        FENNEL_UNIT_TEST_CASE(
            LbmSplicerExecStreamTest, testSpliceRidsSmallSets);
        FENNEL_UNIT_TEST_CASE(
            LbmSplicerExecStreamTest, testSpliceWithKeys50);
        FENNEL_UNIT_TEST_CASE(
            LbmSplicerExecStreamTest, testSpliceWithKeysSmallSpread);
        FENNEL_UNIT_TEST_CASE(
            LbmSplicerExecStreamTest, testSpliceWithKeysLargeSpread);
        FENNEL_UNIT_TEST_CASE(
            LbmSplicerExecStreamTest, testMultipleSpliceWithKeysSmallSpread);
        FENNEL_UNIT_TEST_CASE(
            LbmSplicerExecStreamTest, testMultipleSpliceWithKeysLargeSpread);
        FENNEL_UNIT_TEST_CASE(LbmSplicerExecStreamTest, testLER5968);
        FENNEL_UNIT_TEST_CASE(LbmSplicerExecStreamTest, testLER6473);
    }

    void testCaseSetUp();

    void testSpliceRids50();
    void testSpliceRidsLargeSets();
    void testSpliceRidsSmallSets();
    void testSpliceWithKeys50();
    void testSpliceWithKeysSmallSpread();
    void testSpliceWithKeysLargeSpread();
    void testMultipleSpliceWithKeysSmallSpread();
    void testMultipleSpliceWithKeysLargeSpread();
    void testLER5968();
    void testLER6473();
};

void LbmSplicerExecStreamTest::testSpliceRids50()
{
    testSpliceRids(5, 10);
}

void LbmSplicerExecStreamTest::testSpliceRidsLargeSets()
{
    testSpliceRids(100, 1000);
}

void LbmSplicerExecStreamTest::testSpliceRidsSmallSets()
{
    testSpliceRids(1000, 100);
}

void LbmSplicerExecStreamTest::testSpliceWithKeys50()
{
    testSpliceWithKeys(50, 16, 1, false);
}

void LbmSplicerExecStreamTest::testSpliceWithKeysSmallSpread()
{
    testSpliceWithKeys(50000, 8, 1, false);
}

void LbmSplicerExecStreamTest::testSpliceWithKeysLargeSpread()
{
    testSpliceWithKeys(50000, 24, 1, false);
}

void LbmSplicerExecStreamTest::testMultipleSpliceWithKeysSmallSpread()
{
    testSpliceWithKeys(5000, 8, 1, true);
}

void LbmSplicerExecStreamTest::testMultipleSpliceWithKeysLargeSpread()
{
    testSpliceWithKeys(10000, 24, 1, true);
}

void LbmSplicerExecStreamTest::testSpliceRids(
    uint numSets,
    uint numRidsPerSet)
{
    // Create the btree that the splicer will write into
    BTreeDescriptor bTreeDesc;
    createBTree(bTreeDesc, 0);

    // Generate the rid sets
    uint totalRids = numSets * numRidsPerSet;
    std::vector<std::vector<uint64_t> > ridSets;
    generateSeqRidSets(ridSets, numSets, numRidsPerSet);

    // Splice each set of rids, one per stream graph execution
    for (uint i = 0; i < numSets; i++) {
        // Generate the sequence of specified rids.  Splicer handles
        // individual rid values as input.
        boost::shared_array<FixedBuffer> ridBuffer;
        ridBuffer.reset(new FixedBuffer[numRidsPerSet * 8]);
        uint bufferSize = 0;
        generateRidInput(
            ridSets[i],
            ridBuffer.get(),
            bufferSize);

        spliceInput(
            ridBuffer,
            bufferSize,
            numRidsPerSet,
            bTreeDesc);

        resetExecStreamTest();
    }

    // Read the btree bitmap entries and confirm that they contain all
    // of the rids that were inserted.  The rids should be in a sequence
    // from 0 to totalRids - 1.
    BTreeReader reader(bTreeDesc);
    bool rc = reader.searchFirst();
    BOOST_REQUIRE(rc);
    TupleData tupleData;
    tupleData.compute(bTreeDesc.tupleDescriptor);
    LcsRid startRid = LcsRid(0);
    while (rc) {
        reader.getTupleAccessorForRead().unmarshal(tupleData);
        std::vector<LcsRid> ridsRead;
        LbmEntry::generateRIDs(tupleData, ridsRead);
        for (uint i = 0; i < ridsRead.size(); i++) {
            BOOST_CHECK_EQUAL(
                opaqueToInt(ridsRead[i]),
                opaqueToInt(startRid) + i);
        }
        startRid += ridsRead.size();
        rc = reader.searchNext();
    }
    BOOST_CHECK_EQUAL(opaqueToInt(startRid), totalRids);
}

void LbmSplicerExecStreamTest::generateSeqRidSets(
    std::vector<std::vector<uint64_t> > &ridSets,
    uint numSets,
    uint numRidsPerSet)
{
    // Generate a sequence of rids from 0 to N.  Random shuffle the sequence.
    // Divide them into equal portions.  Then sort within each portion,
    // since splicer expects its rid input in sort order.
    uint totalRids = numSets * numRidsPerSet;
    std::vector<int> rids;
    rids.resize(totalRids);
    for (uint i = 0; i < totalRids; i++) {
        rids[i] = i;
    }

    std::random_shuffle(rids.begin(), rids.end());

    for (uint i = 0; i < numSets; i++) {
        std::vector<uint64_t> ridSet;
        ridSet.resize(numRidsPerSet);
        std::copy(
            rids.begin() + i * numRidsPerSet,
            rids.begin() + (i + 1) * numRidsPerSet,
            ridSet.begin());
        std::sort(ridSet.begin(), ridSet.end());
        ridSets.push_back(ridSet);
    }
}

void LbmSplicerExecStreamTest::generateRidInput(
    std::vector<uint64_t> const &rids,
    PBuffer ridBuffer,
    uint &bufferSize)
{
    TupleData ridTupleData;
    TupleDescriptor ridTupleDesc;
    ridTupleDesc.push_back(attrDesc_int64);
    ridTupleData.compute(ridTupleDesc);

    TupleAccessor ridTupleAccessor;
    ridTupleAccessor.compute(ridTupleDesc);

    for (uint i = 0; i < rids.size(); i++) {
        ridTupleData[0].pData = (PConstBuffer) &rids[i];
        ridTupleAccessor.marshal(ridTupleData, ridBuffer + bufferSize);
        bufferSize += ridTupleAccessor.getCurrentByteCount();
    }
}

void LbmSplicerExecStreamTest::testSpliceWithKeys(
    uint numRows,
    uint factor,
    uint nKeys,
    bool multipleSplices)
{
    // Create the btree that the splicer will write into
    BTreeDescriptor bTreeDesc;
    createBTree(bTreeDesc, nKeys);

    // Generate the random rids
    std::vector<uint64_t> rids;
    generateRandomRids(rids, numRows, factor);

    // Generate tuples containing the rids generated
    boost::shared_array<FixedBuffer> buffer;
    buffer.reset(new FixedBuffer[(nKeys + 2) * numRows * 8]);
    uint currRidIdx = 0;
    do {
        uint bufferSize = 0;
        generateTupleInput(
            rids,
            currRidIdx,
            buffer.get(),
            bufferSize,
            nKeys,
            multipleSplices);
        assert(bufferSize <= (nKeys + 2) * numRows * 8);

        spliceInput(
            buffer,
            bufferSize,
            numRows,
            bTreeDesc);

        resetExecStreamTest();
    } while (currRidIdx < rids.size());

    // Read the btree bitmap entries and confirm that they contain all
    // of the rids that were inserted.
    BTreeReader reader(bTreeDesc);
    bool rc = reader.searchFirst();
    BOOST_REQUIRE(rc);
    TupleData tupleData;
    tupleData.compute(bTreeDesc.tupleDescriptor);
    uint currIdx = 0;
    while (rc) {
        reader.getTupleAccessorForRead().unmarshal(tupleData);
        std::vector<LcsRid> ridsRead;
        LbmEntry::generateRIDs(tupleData, ridsRead);
        for (uint i = 0; i < ridsRead.size(); i++) {
            BOOST_CHECK_EQUAL(
                opaqueToInt(ridsRead[i]),
                opaqueToInt(rids[currIdx]));
            currIdx++;
        }
        rc = reader.searchNext();
    }
    BOOST_CHECK_EQUAL(currIdx, numRows);
}

void LbmSplicerExecStreamTest::testLER5968()
{
    TupleDescriptor tupleDesc;
    initBTreeTupleDesc(tupleDesc, 1);

    // Create the btree that the splicer will write into
    BTreeDescriptor bTreeDesc;
    createBTree(bTreeDesc, 1);

    TupleAccessor tupleAccessor;
    tupleAccessor.compute(tupleDesc);

    // Key values are fixed
    uint64_t keyVal = 1;
    TupleData tupleData;
    tupleData.compute(bTreeDesc.tupleDescriptor);
    tupleData[0].pData = (PConstBuffer) &keyVal;

    LbmEntry lbmEntry;
    boost::scoped_array<FixedBuffer> entryBuf;
    uint entryBufSize = tupleDesc[2].cbStorage + 8 + 8;
    entryBuf.reset(new FixedBuffer[entryBufSize]);
    lbmEntry.init(entryBuf.get(), NULL, entryBufSize, tupleDesc);

    std::vector<uint64_t> rids;

    // First create a singleton entry
    uint64_t rid = 0;
    rids.push_back(rid);
    tupleData[1].pData = (PConstBuffer) &rid;
    tupleData[2].pData = NULL;
    tupleData[2].cbData = 0;
    tupleData[3].pData = NULL;
    tupleData[3].cbData = 0;
    lbmEntry.setEntryTuple(tupleData);
    tupleData = lbmEntry.produceEntryTuple();
    boost::shared_array<FixedBuffer> buffer;
    buffer.reset(new FixedBuffer[1024]);
    uint bufferSize = 0;
    tupleAccessor.marshal(tupleData, buffer.get() + bufferSize);
    bufferSize += tupleAccessor.getCurrentByteCount();

    // Next figure out how many rids are needed to create a full entry
    rid = 5984;
    rids.push_back(rid);
    tupleData[1].pData = (PConstBuffer) &rid;
    tupleData[2].pData = NULL;
    tupleData[2].cbData = 0;
    tupleData[3].pData = NULL;
    tupleData[3].cbData = 0;
    lbmEntry.setEntryTuple(tupleData);
    uint numRids = 1;
    do {
        bool rc = lbmEntry.setRID(LcsRid(rid + numRids * 16));
        if (!rc) {
            break;
        }
        numRids++;
    } while (true);

    // Now, create the actual second entry with a rid count based the count
    // determined above, but with two fewer, so this entry can be spliced with
    // the first.  Note that its startRID is 2 zero-length bytes away from the
    // initial singleton.
    tupleData[1].pData = (PConstBuffer) &rid;
    tupleData[2].pData = NULL;
    tupleData[2].cbData = 0;
    tupleData[3].pData = NULL;
    tupleData[3].cbData = 0;
    lbmEntry.setEntryTuple(tupleData);
    for (int i = 0; i < numRids - 2; i++) {
        rid += 16;
        bool rc = lbmEntry.setRID(LcsRid(rid));
        BOOST_REQUIRE(rc);
        rids.push_back(rid);
    }
    tupleData = lbmEntry.produceEntryTuple();
    tupleAccessor.marshal(tupleData, buffer.get() + bufferSize);
    bufferSize += tupleAccessor.getCurrentByteCount();

    // Create the third entry, again a singleton.
    rid = 10000;
    rids.push_back(rid);
    tupleData[1].pData = (PConstBuffer) &rid;
    tupleData[2].pData = NULL;
    tupleData[2].cbData = 0;
    tupleData[3].pData = NULL;
    tupleData[3].cbData = 0;
    lbmEntry.setEntryTuple(tupleData);
    tupleData = lbmEntry.produceEntryTuple();
    tupleAccessor.marshal(tupleData, buffer.get() + bufferSize);
    bufferSize += tupleAccessor.getCurrentByteCount();

    // Create the fourth entry with a rid count based the count determined
    // above, but with the last rid in a contiguous segment.  This entry,
    // when spliced with the singleton, should result in, an overflow.
    rid = 16984;
    rids.push_back(rid);
    tupleData[1].pData = (PConstBuffer) &rid;
    tupleData[2].pData = NULL;
    tupleData[2].cbData = 0;
    tupleData[3].pData = NULL;
    tupleData[3].cbData = 0;
    lbmEntry.setEntryTuple(tupleData);
    for (int i = 0; i < numRids - 1; i++) {
        rid += 16;
        bool rc = lbmEntry.setRID(LcsRid(rid));
        BOOST_REQUIRE(rc);
        rids.push_back(rid);
    }
    rid += 8;
    bool rc = lbmEntry.setRID(LcsRid(rid));
    rids.push_back(rid);
    tupleData = lbmEntry.produceEntryTuple();
    tupleAccessor.marshal(tupleData, buffer.get() + bufferSize);
    bufferSize += tupleAccessor.getCurrentByteCount();

    // Splice the four entries.  The splice of the first two entries should
    // fit, creating a combined entry.  Then, a singleton should be created.
    // An attempt to splice the fourth entry into the singleton will overflow
    // and therefore create a third entry.
    spliceInput(
        buffer,
        bufferSize,
        rids.size(),
        bTreeDesc);
    resetExecStreamTest();

    // Read the btree bitmap entries and confirm that they contain all
    // of the rids that were inserted.  Explicitly make sure there are
    // three btree entries.
    BTreeReader reader(bTreeDesc);
    rc = reader.searchFirst();
    BOOST_REQUIRE(rc);
    uint currIdx = 0;
    uint numEntries = 0;
    while (rc) {
        numEntries++;
        reader.getTupleAccessorForRead().unmarshal(tupleData);
        std::vector<LcsRid> ridsRead;
        LbmEntry::generateRIDs(tupleData, ridsRead);
        for (uint i = 0; i < ridsRead.size(); i++) {
            BOOST_CHECK_EQUAL(
                opaqueToInt(ridsRead[i]),
                opaqueToInt(rids[currIdx]));
            currIdx++;
        }
        rc = reader.searchNext();
    }
    BOOST_CHECK_EQUAL(currIdx, rids.size());
    BOOST_REQUIRE(numEntries == 3);
}

void LbmSplicerExecStreamTest::testLER6473()
{
    TupleDescriptor tupleDesc;
    initBTreeTupleDesc(tupleDesc, 1);

    // Create the btree that the splicer will write into
    BTreeDescriptor bTreeDesc;
    createBTree(bTreeDesc, 1);

    TupleAccessor tupleAccessor;
    tupleAccessor.compute(tupleDesc);

    // Key values are fixed
    uint64_t keyVal = 1;
    TupleData tupleData;
    tupleData.compute(bTreeDesc.tupleDescriptor);
    tupleData[0].pData = (PConstBuffer) &keyVal;

    LbmEntry lbmEntry;
    boost::scoped_array<FixedBuffer> entryBuf;
    uint entryBufSize = tupleDesc[2].cbStorage + 8 + 8;
    entryBuf.reset(new FixedBuffer[entryBufSize]);
    lbmEntry.init(entryBuf.get(), NULL, entryBufSize, tupleDesc);

    std::vector<uint64_t> rids;

    // First create a bitmap entry at max capacity
    uint64_t rid = 0;
    tupleData[1].pData = (PConstBuffer) &rid;
    tupleData[2].pData = NULL;
    tupleData[2].cbData = 0;
    tupleData[3].pData = NULL;
    tupleData[3].cbData = 0;
    lbmEntry.setEntryTuple(tupleData);
    for (;; rid += 16) {
        bool rc = lbmEntry.setRID(LcsRid(rid));
        if (!rc) {
            break;
        }
        rids.push_back(rid);
    }
    tupleData = lbmEntry.produceEntryTuple();

    boost::shared_array<FixedBuffer> buffer;
    buffer.reset(new FixedBuffer[1024]);
    uint bufferSize = 0;
    tupleAccessor.marshal(tupleData, buffer.get() + bufferSize);
    bufferSize += tupleAccessor.getCurrentByteCount();

    // Then create a singleton that's further away
    rid += 64 + 1;
    rids.push_back(rid);
    tupleData[1].pData = (PConstBuffer) &rid;
    tupleData[2].pData = NULL;
    tupleData[2].cbData = 0;
    tupleData[3].pData = NULL;
    tupleData[3].cbData = 0;
    lbmEntry.setEntryTuple(tupleData);
    tupleData = lbmEntry.produceEntryTuple();
    tupleAccessor.marshal(tupleData, buffer.get() + bufferSize);
    bufferSize += tupleAccessor.getCurrentByteCount();

    // Insert the two bitmap entries we've just created into the btree
    spliceInput(
        buffer,
        bufferSize,
        rids.size(),
        bTreeDesc);
    resetExecStreamTest();

    // Now create a bitmap entry that overlaps with the singleton
    rid += 1;
    rids.push_back(rid);
    tupleData[1].pData = (PConstBuffer) &rid;
    tupleData[2].pData = NULL;
    tupleData[2].cbData = 0;
    tupleData[3].pData = NULL;
    tupleData[3].cbData = 0;
    lbmEntry.setEntryTuple(tupleData);
    for (uint i = 0; i < 5; i++) {
        rid += 8;
        rids.push_back(rid);
        bool rc = lbmEntry.setRID(LcsRid(rid));
        BOOST_REQUIRE(rc);
    }
    tupleData = lbmEntry.produceEntryTuple();
    bufferSize = 0;
    tupleAccessor.marshal(tupleData, buffer.get() + bufferSize);
    bufferSize += tupleAccessor.getCurrentByteCount();

    // Splice that into the existing btree
    spliceInput(
        buffer,
        bufferSize,
        5,
        bTreeDesc);

    // Read the btree bitmap entries and confirm that they contain all
    // of the rids that were inserted.
    BTreeReader reader(bTreeDesc);
    bool rc = reader.searchFirst();
    BOOST_REQUIRE(rc);
    uint currIdx = 0;
    while (rc) {
        reader.getTupleAccessorForRead().unmarshal(tupleData);
        std::vector<LcsRid> ridsRead;
        LbmEntry::generateRIDs(tupleData, ridsRead);
        for (uint i = 0; i < ridsRead.size(); i++) {
            BOOST_CHECK_EQUAL(
                opaqueToInt(ridsRead[i]),
                opaqueToInt(rids[currIdx]));
            currIdx++;
        }
        rc = reader.searchNext();
    }
    BOOST_CHECK_EQUAL(currIdx, rids.size());
}

void LbmSplicerExecStreamTest::generateRandomRids(
    std::vector<uint64_t> &rids,
    uint nRids,
    uint factor)
{
    // Generate "nRids" unique, random rids between 0 and nRids * factor
    std::hash_set<uint64_t> ridsGenerated;
    uint numGenerated = 0;
    while (numGenerated < nRids) {
        uint64_t rid = uint64_t(((double) rand() / RAND_MAX) * nRids * factor);
        if (ridsGenerated.find(rid) == ridsGenerated.end()) {
            rids.push_back(rid);
            ridsGenerated.insert(rid);
            numGenerated++;
        }
    }
    std::sort(rids.begin(), rids.end());
}

void LbmSplicerExecStreamTest::generateTupleInput(
    std::vector<uint64_t> const &rids,
    uint &currRidIdx,
    PBuffer buffer,
    uint &bufferSize,
    uint nKeys,
    bool oneTuple)
{
    TupleDescriptor tupleDesc;
    initBTreeTupleDesc(tupleDesc, nKeys);

    TupleAccessor tupleAccessor;
    tupleAccessor.compute(tupleDesc);

    TupleData tupleData;
    tupleData.compute(tupleDesc);

    // Key values are fixed
    for (uint i = 0; i < nKeys; i++) {
        uint64_t keyVal = i;
        tupleData[i].pData = (PConstBuffer) &keyVal;
    }

    LbmEntry lbmEntry;
    boost::scoped_array<FixedBuffer> entryBuf;
    uint entryBufSize = (nKeys + 1) * 8 + 4 * 3;
    entryBuf.reset(new FixedBuffer[entryBufSize]);
    lbmEntry.init(entryBuf.get(), NULL, entryBufSize, tupleDesc);

    uint numRids = rids.size();
    while (currRidIdx < numRids) {
        tupleData[nKeys].pData = (PConstBuffer) &rids[currRidIdx];
        tupleData[nKeys + 1].pData = NULL;
        tupleData[nKeys + 1].cbData = 0;
        tupleData[nKeys + 2].pData = NULL;
        tupleData[nKeys + 2].cbData = 0;

        // Randomly generate either singletons or bitmaps with 3 rids, except
        // when we're at the end of our rid list
        uint bitmapType;
        if (currRidIdx >= numRids - 3) {
            bitmapType = 0;
        } else {
            bitmapType = rand() % 2;
        }
        if (bitmapType == 0) {
            currRidIdx++;
        } else {
            lbmEntry.setEntryTuple(tupleData);
            bool rc = lbmEntry.setRID(LcsRid(rids[currRidIdx + 1]));
            BOOST_REQUIRE(rc);
            rc = lbmEntry.setRID(LcsRid(rids[currRidIdx + 2]));
            BOOST_REQUIRE(rc);
            tupleData = lbmEntry.produceEntryTuple();
            currRidIdx += 3;
        }

        tupleAccessor.marshal(tupleData, buffer + bufferSize);
        bufferSize += tupleAccessor.getCurrentByteCount();
        if (oneTuple) {
            break;
        }
    }
}

void LbmSplicerExecStreamTest::spliceInput(
    boost::shared_array<FixedBuffer> &inputBuffer,
    uint inputBufSize,
    uint numRows,
    BTreeDescriptor const &bTreeDesc)
{
    // Create a ValuesExecStream that provides input rows to the splicer.
    // In the case of splicing only rids, i.e., no keys, the rids don't need
    // to be passed in as bitmap tuples
    uint nKeys = bTreeDesc.keyProjection.size();
    ValuesExecStreamParams valuesParams;
    for (uint i = 0; i < nKeys; i++) {
        valuesParams.outputTupleDesc.push_back(attrDesc_int64);
    }
    if (nKeys > 1) {
        valuesParams.outputTupleDesc.push_back(attrDesc_bitmap);
        valuesParams.outputTupleDesc.push_back(attrDesc_bitmap);
    }
    valuesParams.pTupleBuffer = inputBuffer,
    valuesParams.bufSize = inputBufSize;

    ExecStreamEmbryo valuesStreamEmbryo;
    valuesStreamEmbryo.init(new ValuesExecStream(), valuesParams);
    valuesStreamEmbryo.getStream()->setName("ValuesExecStream");

    // Create the splicer stream
    LbmSplicerExecStreamParams splicerParams;
    splicerParams.createNewIndex = false;
    splicerParams.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 15);
    splicerParams.pCacheAccessor = pCache;
    BTreeParams bTreeParams;
    initBTreeParam(bTreeParams, bTreeDesc);
    splicerParams.bTreeParams.push_back(bTreeParams);
    splicerParams.outputTupleDesc.push_back(attrDesc_int64);
    splicerParams.writeRowCountParamId = DynamicParamId(0);

    // In the case where there are index keys, we need to create a dynamic
    // parameter that passes in the rowcount of the number of tuples to
    // be spliced
    if (nKeys == 1) {
        splicerParams.insertRowCountParamId = DynamicParamId(0);
    } else {
        splicerParams.insertRowCountParamId = DynamicParamId(1);
        SharedDynamicParamManager pDynamicParamManager =
            pGraph->getDynamicParamManager();
        pDynamicParamManager->createParam(DynamicParamId(1), attrDesc_int64);
        TupleDatum paramValDatum;
        uint64_t rowCount = numRows;
        paramValDatum.pData = (PConstBuffer) &rowCount;
        paramValDatum.cbData = 8;
        pDynamicParamManager->writeParam(DynamicParamId(1), paramValDatum);
    }

    ExecStreamEmbryo splicerStreamEmbryo;
    splicerStreamEmbryo.init(new LbmSplicerExecStream(), splicerParams);
    splicerStreamEmbryo.getStream()->setName("LbmSplicerExecStream");

    SharedExecStream pOutputStream =
        prepareTransformGraph(valuesStreamEmbryo, splicerStreamEmbryo);

    // Setup a generator which expects the number of rows spliced
    RampExecStreamGenerator expectedResultGenerator(numRows);

    verifyOutput(*pOutputStream, 1, expectedResultGenerator);
}

void LbmSplicerExecStreamTest::initBTreeParam(
    BTreeParams &param,
    BTreeDescriptor const &bTreeDesc)
{
    param.pSegment = pRandomSegment;
    param.pRootMap = 0;
    param.tupleDesc = bTreeDesc.tupleDescriptor;
    param.keyProj = bTreeDesc.keyProjection;
    param.pageOwnerId = bTreeDesc.pageOwnerId;
    param.segmentId = bTreeDesc.segmentId;
    param.rootPageId = bTreeDesc.rootPageId;
}

void LbmSplicerExecStreamTest::createBTree(
    BTreeDescriptor &bTreeDesc,
    uint nKeys)
{
    initBTreeTupleDesc(bTreeDesc.tupleDescriptor, nKeys);

    for (uint j = 0; j < nKeys + 1; j++) {
        bTreeDesc.keyProjection.push_back(j);
    }
    bTreeDesc.rootPageId = NULL_PAGE_ID;
    bTreeDesc.segmentAccessor.pSegment = pRandomSegment;
    bTreeDesc.segmentAccessor.pCacheAccessor = pCache;
    BTreeBuilder builder(bTreeDesc, pRandomSegment);
    builder.createEmptyRoot();
    bTreeDesc.rootPageId = builder.getRootPageId();
}

void LbmSplicerExecStreamTest::initBTreeTupleDesc(
    TupleDescriptor &tupleDesc,
    uint nKeys)
{
    for (uint i = 0; i < nKeys + 1; i++) {
        tupleDesc.push_back(attrDesc_int64);
    }
    tupleDesc.push_back(attrDesc_bitmap);
    tupleDesc.push_back(attrDesc_bitmap);
}

void LbmSplicerExecStreamTest::testCaseSetUp()
{
    ExecStreamUnitTestBase::testCaseSetUp();

    attrDesc_int64 = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    // Set the bitmap lengths to a smaller value to force more bitmap splits
    uint varColSize = 64;
    attrDesc_bitmap =
        TupleAttributeDescriptor(
            stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY), true,
            varColSize);
}

FENNEL_UNIT_TEST_SUITE(LbmSplicerExecStreamTest);

// End LbmSplicerExecStreamTest.cpp
