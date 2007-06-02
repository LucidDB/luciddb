/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2003-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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
#include "fennel/test/TestBase.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/common/TraceSource.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>

using namespace fennel;

class StandardTypeTest : virtual public TestBase, public TraceSource
{
    void testStandardTypeToString();
    void testStandardTypeIsNative();
    void testStandardTypeIsNativeNotBool();
    void testStandardTypeIsIntegralNative();
    void testStandardTypeIsExact();
    void testStandardTypeIsApprox();
    void testStandardTypeIsArray();
    void testStandardTypeIsVariableLenArray();
    void testStandardTypeIsFixedLenArray();
    void testStandardTypeIsTextArray();
    void testStandardTypeIsBinaryArray();
    
public:
    explicit StandardTypeTest()
        : TraceSource(shared_from_this(),"StandardTypeTest")
    {
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeToString);
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeIsNative);
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeIsNativeNotBool);
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeIsIntegralNative);
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeIsExact);
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeIsApprox);
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeIsArray);
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeIsVariableLenArray);
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeIsFixedLenArray);
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeIsTextArray);
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeIsBinaryArray);
    }
    
    virtual ~StandardTypeTest()
    {
    }
};

void StandardTypeTest::testStandardTypeToString()
{
    int i;
    StandardTypeDescriptorOrdinal v, x;
    std::string concat;
    const char* str;

    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);
    BOOST_REQUIRE(STANDARD_TYPE_MIN <= STANDARD_TYPE_INT_8);
    BOOST_REQUIRE(STANDARD_TYPE_END > STANDARD_TYPE_VARBINARY);
    
    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        v = StandardTypeDescriptorOrdinal(i);
        str = StandardTypeDescriptor::toString(v);
        x = StandardTypeDescriptor::fromString(str);
        BOOST_CHECK_EQUAL (x, v);
        BOOST_MESSAGE(v << " -> " << str << " -> " << x);
        concat += str;
    }
    BOOST_MESSAGE("concat: |" << concat << "|");
    BOOST_CHECK(!(concat.compare("s1u1s2u2s4u4s8u8bordcvcbvb")));

    for (i = STANDARD_TYPE_VARBINARY; i >= STANDARD_TYPE_INT_8; i--) {
        v = StandardTypeDescriptorOrdinal(i);
        str = StandardTypeDescriptor::toString(v);
        x = StandardTypeDescriptor::fromString(str);
        BOOST_CHECK_EQUAL(x, v);
    }
}

void StandardTypeTest::testStandardTypeIsNative()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);
    BOOST_REQUIRE(STANDARD_TYPE_MIN <= STANDARD_TYPE_INT_8);
    BOOST_REQUIRE(STANDARD_TYPE_END > STANDARD_TYPE_VARBINARY);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        BOOST_MESSAGE("isNative " << i);
        v = StandardTypeDescriptorOrdinal(i);
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

void StandardTypeTest::testStandardTypeIsNativeNotBool()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);
    BOOST_REQUIRE(STANDARD_TYPE_MIN <= STANDARD_TYPE_INT_8);
    BOOST_REQUIRE(STANDARD_TYPE_END > STANDARD_TYPE_VARBINARY);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        BOOST_MESSAGE("isNativeNotBool " << i);
        v = StandardTypeDescriptorOrdinal(i);
        if (v == STANDARD_TYPE_BOOL ||
            v == STANDARD_TYPE_CHAR ||
            v == STANDARD_TYPE_VARCHAR ||
            v == STANDARD_TYPE_BINARY ||
            v == STANDARD_TYPE_VARBINARY) {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isNativeNotBool(v),
                              false);
        } else {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isNativeNotBool(v),
                              true);
        }
    }
}

void StandardTypeTest::testStandardTypeIsIntegralNative()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);
    BOOST_REQUIRE(STANDARD_TYPE_MIN <= STANDARD_TYPE_INT_8);
    BOOST_REQUIRE(STANDARD_TYPE_END > STANDARD_TYPE_VARBINARY);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        BOOST_MESSAGE("isIntegralNative " << i);
        v = StandardTypeDescriptorOrdinal(i);
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

void StandardTypeTest::testStandardTypeIsExact()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);
    BOOST_REQUIRE(STANDARD_TYPE_MIN <= STANDARD_TYPE_INT_8);
    BOOST_REQUIRE(STANDARD_TYPE_END > STANDARD_TYPE_VARBINARY);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        v = StandardTypeDescriptorOrdinal(i);
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

void StandardTypeTest::testStandardTypeIsApprox()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);
    BOOST_REQUIRE(STANDARD_TYPE_MIN <= STANDARD_TYPE_INT_8);
    BOOST_REQUIRE(STANDARD_TYPE_END > STANDARD_TYPE_VARBINARY);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        BOOST_MESSAGE("isApprox " << i);
        v = StandardTypeDescriptorOrdinal(i);
        if (v == STANDARD_TYPE_REAL ||
            v == STANDARD_TYPE_DOUBLE) {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isApprox(v), true);
        } else {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isApprox(v), false);
        }
    }
}


void StandardTypeTest::testStandardTypeIsArray()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);
    BOOST_REQUIRE(STANDARD_TYPE_MIN <= STANDARD_TYPE_INT_8);
    BOOST_REQUIRE(STANDARD_TYPE_END > STANDARD_TYPE_VARBINARY);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        BOOST_MESSAGE("isArray " << i);
        v = StandardTypeDescriptorOrdinal(i);
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

void StandardTypeTest::testStandardTypeIsVariableLenArray()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        BOOST_MESSAGE("isVariableLenArray " << i);
        v = StandardTypeDescriptorOrdinal(i);
        if (v == STANDARD_TYPE_VARCHAR ||
            v == STANDARD_TYPE_VARBINARY) {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isVariableLenArray(v), true);
        } else {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isVariableLenArray(v), false);
        }
    }
}

void StandardTypeTest::testStandardTypeIsFixedLenArray()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        BOOST_MESSAGE("isFixedLenArray " << i);
        v = StandardTypeDescriptorOrdinal(i);
        if (v == STANDARD_TYPE_CHAR ||
            v == STANDARD_TYPE_BINARY) {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isFixedLenArray(v), true);
        } else {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isFixedLenArray(v), false);
        }
    }
}

void StandardTypeTest::testStandardTypeIsTextArray()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        BOOST_MESSAGE("isTextArray " << i);
        v = StandardTypeDescriptorOrdinal(i);
        if (v == STANDARD_TYPE_CHAR ||
            v == STANDARD_TYPE_VARCHAR) {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isTextArray(v), true);
        } else {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isTextArray(v), false);
        }
    }
}

void StandardTypeTest::testStandardTypeIsBinaryArray()
{
    BOOST_REQUIRE(STANDARD_TYPE_MIN < STANDARD_TYPE_END);

    int i;
    StandardTypeDescriptorOrdinal v;

    for (i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; i++) {
        BOOST_MESSAGE("isBinaryArray " << i);
        v = StandardTypeDescriptorOrdinal(i);
        if (v == STANDARD_TYPE_VARBINARY ||
            v == STANDARD_TYPE_BINARY) {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isBinaryArray(v), true);
        } else {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isBinaryArray(v), false);
        }
    }
}



FENNEL_UNIT_TEST_SUITE(StandardTypeTest);

// End TestTuple.cpp

    
