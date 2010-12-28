/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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
#include "fennel/common/PseudoUuid.h"
#include "fennel/common/TraceSource.h"
#include "fennel/test/TestBase.h"
#include <boost/test/test_tools.hpp>
#include <iostream>

using namespace fennel;
using std::string;

/** tests the common PseudoUuid class */

class PseudoUuidTest : public TestBase, public TraceSource
{
    void testGeneration();
    void testInvalid();
    void testComparison();
    void testParsing();
    void testFormatting();
    void testCopy();

public:
    explicit PseudoUuidTest()
        : TraceSource(shared_from_this(), "PseudoUuidTest")
    {
        FENNEL_UNIT_TEST_CASE(PseudoUuidTest, testGeneration);
        FENNEL_UNIT_TEST_CASE(PseudoUuidTest, testInvalid);
        FENNEL_UNIT_TEST_CASE(PseudoUuidTest, testComparison);
        FENNEL_UNIT_TEST_CASE(PseudoUuidTest, testParsing);
        FENNEL_UNIT_TEST_CASE(PseudoUuidTest, testFormatting);
        FENNEL_UNIT_TEST_CASE(PseudoUuidTest, testCopy);
    }
};

void PseudoUuidTest::testGeneration()
{
    PseudoUuid uuid;

    uuid.generate();

    for (int i = 0; i < PseudoUuid::UUID_LENGTH; i++) {
        if (uuid.getByte(i) != 0) {
            // non-zero byte
            return;
        }
    }

    BOOST_ERROR("PseudoUuid::generate() generated all-zero UUID");
}

void PseudoUuidTest::testInvalid()
{
    PseudoUuid uuid;

    uuid.generateInvalid();

    for (int i = 0; i < PseudoUuid::UUID_LENGTH; i++) {
        BOOST_CHECK_MESSAGE(
            uuid.getByte(i) == (uint8_t)0xFF, "invalid UUID not all 0xFF");
    }
}

void PseudoUuidTest::testComparison()
{
    PseudoUuid uuid1("00010203-0405-0607-0809-0A0B0C0D0E0F");
    PseudoUuid uuid2("00010203-0405-0607-0809-0A0B0C0D0E0F");
    PseudoUuid uuid3("0F0E0D0C-0B0A-0908-0706-050403020100");

    BOOST_CHECK(uuid1 == uuid2);
    BOOST_CHECK(uuid1 != uuid3);
    BOOST_CHECK(uuid2 != uuid3);
}

void PseudoUuidTest::testParsing()
{
    PseudoUuid uuid1("00010203-0405-0607-0809-0A0B0C0D0E0F");

    for (int i = 0; i < PseudoUuid::UUID_LENGTH; i++) {
        BOOST_CHECK_EQUAL(i, uuid1.getByte(i));
    }

    PseudoUuid uuid2("00000000-0000-0000-0000-000000000000");

    for (int i = 0; i < PseudoUuid::UUID_LENGTH; i++) {
        BOOST_CHECK_EQUAL(0, uuid2.getByte(i));
    }

    PseudoUuid uuid3("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF");

    for (int i = 0; i < PseudoUuid::UUID_LENGTH; i++) {
        BOOST_CHECK_EQUAL((uint8_t)0xff, uuid3.getByte(i));
    }

    BOOST_CHECK_THROW(
        PseudoUuid("bad"), FennelExcn);
    BOOST_CHECK_THROW(
        PseudoUuid("00112233-44-55-6677-8899-AABBCCDDEEFF"), FennelExcn);
}

void PseudoUuidTest::testFormatting()
{
    string exp1 = "12345678-9abc-def0-1234-56789abcdef0";
    string exp2 = "00000000-0000-0000-0000-000000000000";

    PseudoUuid uuid1(exp1), uuid2(exp2);

    string got1 = uuid1.toString();
    string got2 = uuid2.toString();

    BOOST_CHECK_EQUAL(exp1, got1);
    BOOST_CHECK_EQUAL(exp2, got2);
}

void PseudoUuidTest::testCopy()
{
    PseudoUuid uuid1("00010203-0405-0607-0809-0A0B0C0D0E0F");
    PseudoUuid uuid2(uuid1);
    PseudoUuid uuid3 = uuid1;

    BOOST_CHECK_EQUAL(uuid1, uuid2);
    BOOST_CHECK_EQUAL(uuid1, uuid3);

    PseudoUuid uuid4;

    uuid4.generateInvalid();

    PseudoUuid uuid5 = uuid4;
    PseudoUuid uuid6(uuid4);

    BOOST_CHECK_EQUAL(uuid4, uuid5);
    BOOST_CHECK_EQUAL(uuid4, uuid6);
}
FENNEL_UNIT_TEST_SUITE(PseudoUuidTest)
// End PseudoUuidTest.cpp

