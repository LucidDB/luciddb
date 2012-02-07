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
        : TraceSource(shared_from_this(), "StandardTypeTest")
    {
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeToString);
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeIsNative);
        FENNEL_UNIT_TEST_CASE(
            StandardTypeTest, testStandardTypeIsNativeNotBool);
        FENNEL_UNIT_TEST_CASE(
            StandardTypeTest, testStandardTypeIsIntegralNative);
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeIsExact);
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeIsApprox);
        FENNEL_UNIT_TEST_CASE(StandardTypeTest, testStandardTypeIsArray);
        FENNEL_UNIT_TEST_CASE(
            StandardTypeTest, testStandardTypeIsVariableLenArray);
        FENNEL_UNIT_TEST_CASE(
            StandardTypeTest, testStandardTypeIsFixedLenArray);
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
        BOOST_CHECK_EQUAL(x, v);
        BOOST_MESSAGE(v << " -> " << str << " -> " << x);
        concat += str;
    }
    BOOST_MESSAGE("concat: |" << concat << "|");
    BOOST_CHECK(!(concat.compare("s1u1s2u2s4u4s8u8bordcvcbvbUvU")));

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
        if (v == STANDARD_TYPE_CHAR
            || v == STANDARD_TYPE_VARCHAR
            || v == STANDARD_TYPE_BINARY
            || v == STANDARD_TYPE_VARBINARY
            || v == STANDARD_TYPE_UNICODE_CHAR
            || v == STANDARD_TYPE_UNICODE_VARCHAR)
        {
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
        if (v == STANDARD_TYPE_BOOL
            || v == STANDARD_TYPE_CHAR
            || v == STANDARD_TYPE_VARCHAR
            || v == STANDARD_TYPE_BINARY
            || v == STANDARD_TYPE_VARBINARY
            || v == STANDARD_TYPE_UNICODE_CHAR
            || v == STANDARD_TYPE_UNICODE_VARCHAR)
        {
            BOOST_CHECK_EQUAL(
                StandardTypeDescriptor::isNativeNotBool(v),
                false);
        } else {
            BOOST_CHECK_EQUAL(
                StandardTypeDescriptor::isNativeNotBool(v),
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
        if (v == STANDARD_TYPE_REAL
            || v == STANDARD_TYPE_DOUBLE
            || v == STANDARD_TYPE_CHAR
            || v == STANDARD_TYPE_VARCHAR
            || v == STANDARD_TYPE_BINARY
            || v == STANDARD_TYPE_VARBINARY
            || v == STANDARD_TYPE_UNICODE_CHAR
            || v == STANDARD_TYPE_UNICODE_VARCHAR)
        {
            BOOST_CHECK_EQUAL(
                StandardTypeDescriptor::isIntegralNative(v),
                false);
        } else {
            BOOST_CHECK_EQUAL(
                StandardTypeDescriptor::isIntegralNative(v),
                true);
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
        BOOST_MESSAGE(
            "isExact " << i << " " << StandardTypeDescriptor::isExact(v));
        if (v == STANDARD_TYPE_BOOL
            || v == STANDARD_TYPE_REAL
            || v == STANDARD_TYPE_DOUBLE
            || v == STANDARD_TYPE_CHAR
            || v == STANDARD_TYPE_VARCHAR
            || v == STANDARD_TYPE_BINARY
            || v == STANDARD_TYPE_VARBINARY
            || v == STANDARD_TYPE_UNICODE_CHAR
            || v == STANDARD_TYPE_UNICODE_VARCHAR)
        {
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
        if (v == STANDARD_TYPE_REAL
            || v == STANDARD_TYPE_DOUBLE)
        {
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
        if (v == STANDARD_TYPE_CHAR
            || v == STANDARD_TYPE_VARCHAR
            || v == STANDARD_TYPE_BINARY
            || v == STANDARD_TYPE_VARBINARY
            || v == STANDARD_TYPE_UNICODE_CHAR
            || v == STANDARD_TYPE_UNICODE_VARCHAR)
        {
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
        if (v == STANDARD_TYPE_VARCHAR
            || v == STANDARD_TYPE_VARBINARY
            || v == STANDARD_TYPE_UNICODE_VARCHAR)
        {
            BOOST_CHECK_EQUAL(
                StandardTypeDescriptor::isVariableLenArray(v), true);
        } else {
            BOOST_CHECK_EQUAL(
                StandardTypeDescriptor::isVariableLenArray(v), false);
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
        if (v == STANDARD_TYPE_CHAR
            || v == STANDARD_TYPE_BINARY
            || v == STANDARD_TYPE_UNICODE_CHAR)
        {
            BOOST_CHECK_EQUAL(
                StandardTypeDescriptor::isFixedLenArray(v),
                true);
        } else {
            BOOST_CHECK_EQUAL(
                StandardTypeDescriptor::isFixedLenArray(v),
                false);
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
        if (v == STANDARD_TYPE_CHAR
            || v == STANDARD_TYPE_VARCHAR
            || v == STANDARD_TYPE_UNICODE_CHAR
            || v == STANDARD_TYPE_UNICODE_VARCHAR)
        {
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
        if (v == STANDARD_TYPE_VARBINARY
            || v == STANDARD_TYPE_BINARY)
        {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isBinaryArray(v), true);
        } else {
            BOOST_CHECK_EQUAL(StandardTypeDescriptor::isBinaryArray(v), false);
        }
    }
}



FENNEL_UNIT_TEST_SUITE(StandardTypeTest);

// End StandardTypeTest.cpp
