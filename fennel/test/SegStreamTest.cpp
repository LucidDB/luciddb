/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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

    void testWrite(SharedByteOutputStream pOutputStream,uint nRuns)
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
                maxWordLength = std::max(word.size(),maxWordLength);
                if (i == 0) {
                    totalDictLength += word.size();
                }
                if (i < 2) {
                    pOutputStream->writeBytes(word.c_str(),word.size());
                } else {
                    uint cbActual;
                    PBuffer pBuf = pOutputStream->getWritePointer(
                        word.size(),&cbActual);
                    BOOST_CHECK(cbActual >= word.size());
                    memcpy(pBuf,word.c_str(),word.size());
                    pOutputStream->consumeWritePointer(word.size());
                }
            }
        }
    }

    void testRead(SharedByteInputStream pInputStream,uint nRuns)
    {
        boost::scoped_array<char> wordArray(new char[maxWordLength+1]);
        for (uint i = 0; i < nRuns; ++i) {
            if (i == 0) {
                pInputStream->seekForward(totalDictLength);
                continue;
            }
            std::ifstream wordStream(
                configMap.getStringParam(paramDictionaryFileName).c_str());
            std::string word,segWord;
            for (;;) {
                word.clear();
                wordStream >> word;
                if (word == "") {
                    break;
                }
                segWord.clear();
                if (i < 2) {
                    uint nChars = pInputStream->readBytes(
                        wordArray.get(),word.size());
                    BOOST_CHECK_EQUAL(nChars,word.size());
                    segWord.assign(wordArray.get(),word.size());
                } else {
                    uint cbActual;
                    PConstBuffer pBuf = pInputStream->getReadPointer(
                        word.size(),&cbActual);
                    BOOST_CHECK(pBuf);
                    BOOST_CHECK(cbActual >= word.size());
                    segWord.assign(
                        reinterpret_cast<char const *>(pBuf),
                        word.size());
                    pInputStream->consumeReadPointer(word.size());
                }
                BOOST_CHECK_EQUAL(word,segWord);
            }
        }
        uint nChars = pInputStream->readBytes(wordArray.get(),1);
        BOOST_CHECK(!nChars);
        BOOST_CHECK(!pInputStream->getReadPointer(1));
    }
    
public:
    explicit SegStreamTest()
    {
        // Grow page-by-page since preallocation will result in garbage at end
        // of stream.
        nDiskPages = 0;
        
        FENNEL_UNIT_TEST_CASE(SegStreamTest,testWriteSeg);
        FENNEL_UNIT_TEST_CASE(SegStreamTest,testReadSeg);
        FENNEL_UNIT_TEST_CASE(SegStreamTest,testWriteSpillAndRead);
    }

    void testWriteSeg()
    {
        openStorage(DeviceMode::createNew);
        SegmentAccessor segmentAccessor(pLinearSegment,pCache);
        SharedSegOutputStream pOutputStream =
            SegOutputStream::newSegOutputStream(segmentAccessor);
        testWrite(pOutputStream,3);
        pOutputStream.reset();
        segmentAccessor.reset();
        closeStorage();
    }

    void testReadSeg()
    {
        openStorage(DeviceMode::load);
        SegmentAccessor segmentAccessor(pLinearSegment,pCache);
        SharedSegInputStream pInputStream =
            SegInputStream::newSegInputStream(segmentAccessor);
        pInputStream->startPrefetch();
        testRead(pInputStream,3);
        pInputStream.reset();
        segmentAccessor.reset();
        closeStorage();
    }
    
    void testWriteSpillAndRead()
    {
        openStorage(DeviceMode::createNew);
        SharedSpillOutputStream pOutputStream =
            SpillOutputStream::newSpillOutputStream(
                pSegmentFactory,pCache,"spill.dat");
        testWrite(pOutputStream,2);
        SharedByteInputStream pInputStream = pOutputStream->getInputStream();
        pOutputStream.reset();
        testRead(pInputStream,2);
        pInputStream.reset();
        closeStorage();
    }
    
};

FENNEL_UNIT_TEST_SUITE(SegStreamTest);

// End SegStreamTest.cpp
