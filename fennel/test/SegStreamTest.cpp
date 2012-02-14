/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/SegStorageTestBase.h"
#include "fennel/segment/SegOutputStream.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SpillOutputStream.h"

#include <fstream>
#include <boost/scoped_array.hpp>
#include <boost/test/test_tools.hpp>

using namespace fennel;

class SegStreamTest : virtual public SegStorageTestBase
{
    size_t maxWordLength;
    uint totalDictLength;

    void testWrite(SharedByteOutputStream pOutputStream, uint nRuns)
    {
        maxWordLength = 0;
        totalDictLength = 0;
        for (uint i = 0; i < nRuns; ++i) {
            std::ifstream wordStream(
                configMap.getStringParam(paramDictionaryFileName).c_str());
            BOOST_CHECK(wordStream.good());
            std::string word;
            for (;;) {
                word.clear();
                wordStream >> word;
                if (word == "") {
                    break;
                }
                maxWordLength = std::max(word.size(), maxWordLength);
                if (i == 0) {
                    totalDictLength += word.size();
                }
                if (i < 2) {
                    pOutputStream->writeBytes(word.c_str(), word.size());
                } else {
                    uint cbActual;
                    PBuffer pBuf = pOutputStream->getWritePointer(
                        word.size(), &cbActual);
                    BOOST_CHECK(cbActual >= word.size());
                    memcpy(pBuf, word.c_str(), word.size());
                    pOutputStream->consumeWritePointer(word.size());
                }
            }
        }
    }

    void testRead(SharedByteInputStream pInputStream, uint nRuns)
    {
        boost::scoped_array<char> wordArray(new char[maxWordLength + 1]);
        for (uint i = 0; i < nRuns; ++i) {
            if (i == 0) {
                pInputStream->seekForward(totalDictLength);
                continue;
            }
            std::ifstream wordStream(
                configMap.getStringParam(paramDictionaryFileName).c_str());
            std::string word, segWord;
            for (;;) {
                word.clear();
                wordStream >> word;
                if (word == "") {
                    break;
                }
                segWord.clear();
                if (i < 2) {
                    uint nChars = pInputStream->readBytes(
                        wordArray.get(), word.size());
                    BOOST_CHECK_EQUAL(nChars, word.size());
                    segWord.assign(wordArray.get(), word.size());
                } else {
                    uint cbActual;
                    PConstBuffer pBuf = pInputStream->getReadPointer(
                        word.size(), &cbActual);
                    BOOST_CHECK(pBuf);
                    BOOST_CHECK(cbActual >= word.size());
                    segWord.assign(
                        reinterpret_cast<char const *>(pBuf),
                        word.size());
                    pInputStream->consumeReadPointer(word.size());
                }
                BOOST_CHECK_EQUAL(word, segWord);
            }
        }
        uint nChars = pInputStream->readBytes(wordArray.get(), 1);
        BOOST_CHECK(!nChars);
        BOOST_CHECK(!pInputStream->getReadPointer(1));
    }

public:
    explicit SegStreamTest()
    {
        // Grow page-by-page since preallocation will result in garbage at end
        // of stream.
        nDiskPages = 0;

        FENNEL_UNIT_TEST_CASE(SegStreamTest, testWriteSeg);
        FENNEL_UNIT_TEST_CASE(SegStreamTest, testReadSeg);
        FENNEL_UNIT_TEST_CASE(SegStreamTest, testMarkReset);
        FENNEL_UNIT_TEST_CASE(SegStreamTest, testWriteSpillAndRead);
    }

    void testWriteSeg()
    {
        openStorage(DeviceMode::createNew);
        SegmentAccessor segmentAccessor(pLinearSegment, pCache);
        SharedSegOutputStream pOutputStream =
            SegOutputStream::newSegOutputStream(segmentAccessor);
        testWrite(pOutputStream, 3);
        pOutputStream.reset();
        segmentAccessor.reset();
        closeStorage();
    }

    void testReadSeg()
    {
        openStorage(DeviceMode::load);
        SegmentAccessor segmentAccessor(pLinearSegment, pCache);
        SharedSegInputStream pInputStream =
            SegInputStream::newSegInputStream(segmentAccessor);
        pInputStream->startPrefetch();
        testRead(pInputStream, 3);
        pInputStream.reset();
        segmentAccessor.reset();
        closeStorage();
    }

    void testMarkReset()
    {
        openStorage(DeviceMode::load);
        SegmentAccessor segmentAccessor(pLinearSegment, pCache);
        SharedSegInputStream pInputStream =
            SegInputStream::newSegInputStream(segmentAccessor);
        SharedByteStreamMarker pMarker = pInputStream->newMarker();
        pInputStream->mark(*pMarker);
        pInputStream->startPrefetch();
        testRead(pInputStream, 3);
        pInputStream->reset(*pMarker);
        testRead(pInputStream, 3);
        pInputStream.reset();
        segmentAccessor.reset();
        closeStorage();
    }

    void testWriteSpillAndRead()
    {
        openStorage(DeviceMode::createNew);
        SharedSpillOutputStream pOutputStream =
            SpillOutputStream::newSpillOutputStream(
                pSegmentFactory, pCache, "spill.dat");
        testWrite(pOutputStream, 2);
        SharedByteInputStream pInputStream = pOutputStream->getInputStream();
        pOutputStream.reset();
        testRead(pInputStream, 2);
        pInputStream.reset();
        closeStorage();
    }

};

FENNEL_UNIT_TEST_SUITE(SegStreamTest);

// End SegStreamTest.cpp
