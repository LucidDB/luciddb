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
#include "fennel/test/TestBase.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/common/TraceSource.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>

using namespace fennel;

class TestStandardType : virtual public TestBase, public TraceSource
{
    void testStandardTypeToString();
    void testStandardTypeIsNative();
    void testStandardTypeIsIntegralNative();
    void testStandardTypeIsExact();
    void testStandardTypeIsApprox();
    void testStandardTypeIsArray();
    
public:
    explicit TestStandardType()
        : TraceSource(this,"TestStandardType")
    {
        FENNEL_UNIT_TEST_CASE(TestStandardType, testStandardTypeToString);
        FENNEL_UNIT_TEST_CASE(TestStandardType, testStandardTypeIsNative);
        FENNEL_UNIT_TEST_CASE(TestStandardType, testStandardTypeIsIntegralNative);
        FENNEL_UNIT_TEST_CASE(TestStandardType, testStandardTypeIsExact);
        FENNEL_UNIT_TEST_CASE(TestStandardType, testStandardTypeIsApprox);
        FENNEL_UNIT_TEST_CASE(TestStandardType, testStandardTypeIsArray);
    }
    
    virtual ~TestStandardType()
    {
    }
};

void TestStandardType::testStandardTypeToString()
{
    int i;
    StandardTypeDescriptorOrdinal v, x;
    std::string concat;
    const char* str;

    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);
    BOOST_REQUIRE(STANDARD_TYPE_MIN <= STANDARD_TYPE_INT_8);
    BOOST_REQUIRE(STANDARD_TYPE_END > STANDARD_TYPE_VARBINARY);
    
    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        v = *(reinterpret_cast<StandardTypeDescriptorOrdinal *>(&i));
        str = StandardTypeDescriptor::toString(v);
        x = StandardTypeDescriptor::fromString(str);
        BOOST_CHECK_EQUAL (x, v);
        BOOST_MESSAGE(v << " -> " << str << " -> " << x);
        concat += str;
    }
    BOOST_MESSAGE("concat: |" << concat << "|");
    BOOST_CHECK(!(concat.compare("s1u1s2u2s4u4s8u8bordcvcbvb")));

    for (i = STANDARD_TYPE_VARBINARY; i >= STANDARD_TYPE_INT_8; i--) {
        v = *(reinterpret_cast<StandardTypeDescriptorOrdinal *>(&i));
        str = StandardTypeDescriptor::toString(v);
        x = StandardTypeDescriptor::fromString(str);
        BOOST_CHECK_EQUAL(x, v);
    }
}

void TestStandardType::testStandardTypeIsNative()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);
    BOOST_REQUIRE(STANDARD_TYPE_MIN <= STANDARD_TYPE_INT_8);
    BOOST_REQUIRE(STANDARD_TYPE_END > STANDARD_TYPE_VARBINARY);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        BOOST_MESSAGE("isNative " << i);
        v = *(reinterpret_cast<StandardTypeDescriptorOrdinal *>(&i));
        if (v == STANDARD_TYPE_CHAR ||
            v == STANDARD_TYPE_VARCHAR ||
            v == STANDARD_TYPE_BINARY ||
            v == STANDARD_TYPE_VARBINARY) {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isNative(v), false);
        } else {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isNative(v), true);
        }
    }
}

void TestStandardType::testStandardTypeIsIntegralNative()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);
    BOOST_REQUIRE(STANDARD_TYPE_MIN <= STANDARD_TYPE_INT_8);
    BOOST_REQUIRE(STANDARD_TYPE_END > STANDARD_TYPE_VARBINARY);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        BOOST_MESSAGE("isIntegralNative " << i);
        v = *(reinterpret_cast<StandardTypeDescriptorOrdinal *>(&i));
        if (v == STANDARD_TYPE_REAL ||
            v == STANDARD_TYPE_DOUBLE ||
            v == STANDARD_TYPE_CHAR ||
            v == STANDARD_TYPE_VARCHAR ||
            v == STANDARD_TYPE_BINARY ||
            v == STANDARD_TYPE_VARBINARY) {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isIntegralNative(v), false);
        } else {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isIntegralNative(v), true);
        }
    }
}

void TestStandardType::testStandardTypeIsExact()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);
    BOOST_REQUIRE(STANDARD_TYPE_MIN <= STANDARD_TYPE_INT_8);
    BOOST_REQUIRE(STANDARD_TYPE_END > STANDARD_TYPE_VARBINARY);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        v = *(reinterpret_cast<StandardTypeDescriptorOrdinal *>(&i));
        BOOST_MESSAGE("isExact " << i << " " << StandardTypeDescriptor::isExact(v));
        if (v == STANDARD_TYPE_BOOL ||
            v == STANDARD_TYPE_REAL ||
            v == STANDARD_TYPE_DOUBLE ||
            v == STANDARD_TYPE_CHAR ||
            v == STANDARD_TYPE_VARCHAR ||
            v == STANDARD_TYPE_BINARY ||
            v == STANDARD_TYPE_VARBINARY) {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isExact(v), false);
        } else {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isExact(v), true);
        }
    }
}

void TestStandardType::testStandardTypeIsApprox()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);
    BOOST_REQUIRE(STANDARD_TYPE_MIN <= STANDARD_TYPE_INT_8);
    BOOST_REQUIRE(STANDARD_TYPE_END > STANDARD_TYPE_VARBINARY);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        BOOST_MESSAGE("isApprox " << i);
        v = *(reinterpret_cast<StandardTypeDescriptorOrdinal *>(&i));
        if (v == STANDARD_TYPE_REAL ||
            v == STANDARD_TYPE_DOUBLE) {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isApprox(v), true);
        } else {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isApprox(v), false);
        }
    }
}


void TestStandardType::testStandardTypeIsArray()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);
    BOOST_REQUIRE(STANDARD_TYPE_MIN <= STANDARD_TYPE_INT_8);
    BOOST_REQUIRE(STANDARD_TYPE_END > STANDARD_TYPE_VARBINARY);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        BOOST_MESSAGE("isArray " << i);
        v = *(reinterpret_cast<StandardTypeDescriptorOrdinal *>(&i));
        if (v == STANDARD_TYPE_CHAR ||
            v == STANDARD_TYPE_VARCHAR ||
            v == STANDARD_TYPE_BINARY ||
            v == STANDARD_TYPE_VARBINARY) {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isArray(v), true);
        } else {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isArray(v), false);
        }
    }
}



FENNEL_UNIT_TEST_SUITE(TestStandardType);

// End TestTuple.cpp

    
