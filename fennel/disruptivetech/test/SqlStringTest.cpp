/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Disruptive Tech
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/TestBase.h"
#include "fennel/disruptivetech/calc/SqlString.h"
#include "fennel/common/TraceSource.h"
#include "fennel/disruptivetech/test/SqlStringBuffer.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>
#include <iostream>
#include <math.h>

#ifdef HAVE_ICU
#include <unicode/unistr.h>
#include <unicode/uloc.h>
#endif

using namespace fennel;
using namespace std;

#ifdef COMPLETE_REGRESSION_TEST_SETTINGS
// full regression settings 
const int MAXLEN = 8;   // Must not be less than 5. Best >=7.
const int MAXRANDOM = 5;  // 5-8 offers good coverage
const int MAXCMPRANDOM = 65536; // Must be nearly 2^16 char set coverage
const int MAXCMPLEN = 8;  // Must not be less than 3.
#else
// faster check-in acceptance testing settings
const int MAXLEN = 5;   // Must not be less than 5. Best >=7.
const int MAXRANDOM = 1;
const int MAXCMPRANDOM = 256; // Must be nearly 2^16 char set coverage
const int MAXCMPLEN = 3;  // Must not be less than 3.
#endif

class SqlStringTest : virtual public TestBase, public TraceSource
{
    void testSqlStringBuffer_Ascii();
    void testSqlStringBuffer_UCS2();

    void testSqlStringCat_Fix();
    void testSqlStringCat_Var();
    void testSqlStringCat_Var2();
    void testSqlStringCpy_Fix();
    void testSqlStringCpy_Var();
    void testSqlStringCmp();
    void testSqlStringCmp_Bin();
    void testSqlStringLenBit();
    void testSqlStringLenChar();
    void testSqlStringLenOct();
    void testSqlStringOverlay();
    void testSqlStringPos();
    void testSqlStringSubStr();
    void testSqlStringAlterCase();
    void testSqlStringTrim();
    void testSqlStringCastToExact();
    void testSqlStringCastToDecimal();
    void testSqlStringCastToApprox();
    void testSqlStringCastFromExact();
    void testSqlStringCastFromDecimal();
    void testSqlStringCastFromApprox();
    void testSqlStringCastToVarChar();
    void testSqlStringCastToChar();

    void appendCharsToUCS2LikeString(string& str,
                                     int number,
                                     char character);
    
    void testSqlStringCmp_Helper(SqlStringBuffer &src1,
                                 int src1_storage,
                                 int src1_len,
                                 SqlStringBuffer &src2,
                                 int src2_storage,
                                 int src2_len);
    void testSqlStringCmp_Bin_Helper(SqlStringBuffer &src1,
                                     int src1_storage,
                                     int src1_len,
                                     SqlStringBuffer &src2,
                                     int src2_storage,
                                     int src2_len);
    void testSqlStringCmp_Bin_Helper2(int &lower, int &upper);
    int testSqlStringNormalizeLexicalCmp(int v);

    void testSqlStringAlterCase_Ascii(int dst_storage,
                                      int src_len,
                                      SqlStringBuffer& dest,
                                      SqlStringBuffer& src,
                                      const string& expect,
                                      SqlStrAlterCaseAction action);
    
    void testSqlStringAlterCase_UCS2(int dst_storage,
                                     int src_len,
                                     SqlStringBufferUCS2& destU2,
                                     SqlStringBufferUCS2& srcU2,
                                     const string& expect,
                                     SqlStrAlterCaseAction action);
    
    void testSqlStringAlterCase_Case(SqlStrAlterCaseAction action,
                                     int dst_storage,
                                     int dest_len,
                                     int src_storage,
                                     int src_len);

    void testSqlStringTrim_Helper(int dst_storage,
                                  int src_storage,
                                  int src_len,
                                  int leftpad,
                                  int rightpad,
                                  int action);

    void testSqlStringCastToExact_Helper(uint64_t value,
                                         char const * const buf,
                                         int src_storage,
                                         int src_len,
                                         bool exceptionExpected);

    void testSqlStringCastToDecimal_Helper(uint64_t value,
                                           int precision,
                                           int scale,
                                           char const * const buf,
                                           int src_storage,
                                           int src_len,
                                           bool outOfRangeExpected,
                                           bool invalidCharExpected);

    void testSqlStringCastToApprox_Helper(double value,
                                          char const * const buf,
                                          int src_storage,
                                          int src_len,
                                          bool exceptionExpected);
#ifdef HAVE_ICU
    string UnicodeToPrintable(const UnicodeString &s);
#endif

    
public:
    explicit SqlStringTest()
        : TraceSource(shared_from_this(),"SqlStringTest")
    {
        srand(time(NULL));
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringBuffer_Ascii);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringBuffer_UCS2);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCat_Fix);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCat_Var2);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCat_Var);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCpy_Fix);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCpy_Var);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCmp);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCmp_Bin);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringLenBit);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringLenChar);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringLenOct);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringOverlay);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringPos);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringSubStr);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAlterCase);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringTrim);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCastToExact);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCastToDecimal);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCastToApprox);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCastFromExact);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCastFromDecimal);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCastFromApprox);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCastToVarChar);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringCastToChar);
    }
    
    virtual ~SqlStringTest()
    {
    }
};

#ifdef HAVE_ICU
// Note: Assumes that only ASCII chars are represented
string
SqlStringTest::UnicodeToPrintable(const UnicodeString &s) {
    ostringstream o;
    int32_t i, length;
    char tmp;
    
    // output the code units (not code points)
    length = s.length();
    for (i=0; i<length; ++i) {
        tmp = s.charAt(i) & 0xff;
        o << i << "=" << tmp << " | ";
    }
    return o.str();
}
#endif

// build these strings by hand, not using ICU, so we're also
// testing that ICU is integrated and working correctly.
void
SqlStringTest::appendCharsToUCS2LikeString(string& str,
                                           int number,
                                           char character)
{
    int i;
    for (i = 0; i < number; i++) {
#ifdef LITTLEENDIAN
        str.push_back(character);
        str.push_back(0);
#else
        str.push_back(0);
        str.push_back(character);
#endif
    }
}


void
SqlStringTest::testSqlStringBuffer_Ascii()
{
    int storage, size, leftpad, rightpad;
    int leftbump = 2;
    int rightbump = 2;
    int k;
    
    for (storage = 0; storage <= 5; storage++) {
        for (size = 0; size <= storage; size++) {
            for (leftpad = 0; leftpad <= storage-size; leftpad++) {
                rightpad = (storage-size) - leftpad;

                SqlStringBuffer t(storage, size,
                                  leftpad, rightpad,
                                  'x', ' ', 
                                  leftbump, rightbump);

                BOOST_CHECK_EQUAL(t.mStorage, storage);
                BOOST_CHECK_EQUAL(t.mSize, size);
                BOOST_CHECK_EQUAL(t.mLeftPad, leftpad);
                BOOST_CHECK_EQUAL(t.mRightPad, rightpad);
                BOOST_CHECK_EQUAL(static_cast<int>(t.mS.size()), storage+leftbump+rightbump);
                
                BOOST_CHECK(t.verify());

                char *p = t.mLeftP;
                // left bumper
                for (k = 0; k < leftbump; k++) {
                    BOOST_CHECK_EQUAL(*(p++), SqlStringBuffer::mBumperChar);
                }
                BOOST_CHECK(p == t.mStr);
                // left padding
                for (k = 0; k < leftpad; k++) {
                    BOOST_CHECK_EQUAL(*(p++), ' ');
                }
                // text
                for (k = 0; k < size; k++) {
                    BOOST_CHECK_EQUAL(*(p++), 'x');
                }
                // right padding
                for (k = 0; k < rightpad; k++) {
                    BOOST_CHECK_EQUAL(*(p++), ' ');
                }
                BOOST_CHECK(p == t.mRightP);
                // right bumper
                for (k = 0; k < rightbump; k++) {
                    BOOST_CHECK_EQUAL(*(p++), SqlStringBuffer::mBumperChar);
                }
                BOOST_CHECK_EQUAL(static_cast<int>(p - t.mLeftP), storage+leftbump+rightbump);
        
                BOOST_CHECK(t.verify());

                for (k = 0; k < size; k++) {
                    *(t.mStr+k) = '0' + (k % 10);
                }
                BOOST_CHECK(t.verify());

                *(t.mLeftP) = 'X';
                BOOST_CHECK(!t.verify());
                *(t.mLeftP) = SqlStringBuffer::mBumperChar;
                BOOST_CHECK(t.verify());

                *(t.mStr - 1) = 'X';
                BOOST_CHECK(!t.verify());
                *(t.mStr - 1) = SqlStringBuffer::mBumperChar;
                BOOST_CHECK(t.verify());

                *(t.mRightP) = 'X';
                BOOST_CHECK(!t.verify());
                *(t.mRightP) = SqlStringBuffer::mBumperChar;
                BOOST_CHECK(t.verify());

                *(t.mRightP + t.mRightBump - 1) = 'X';
                BOOST_CHECK(!t.verify());
                *(t.mRightP + t.mRightBump - 1) = SqlStringBuffer::mBumperChar;
                BOOST_CHECK(t.verify());

                t.randomize();
                BOOST_CHECK(t.verify());
            }
        }
    }
}

void
SqlStringTest::testSqlStringBuffer_UCS2()
{
    int storage, size, leftpad, rightpad;
    int leftbump = 2;
    int rightbump = 2;
    int k;

    int textChar = 'x';
    int spaceChar = ' ';
    char textChar1, textChar2, spaceChar1, spaceChar2;
#ifdef LITTLEENDIAN
    textChar2 = (textChar >> 8) & 0xff;
    textChar1 = textChar & 0xff;
    spaceChar2 = (spaceChar >> 8) & 0xff;
    spaceChar1 = spaceChar & 0xff;
#elif BIGENDIAN
    textChar1 = (textChar >> 8) & 0xff;
    textChar2 = textChar & 0xff;
    spaceChar1 = (spaceChar >> 8) & 0xff;
    spaceChar2 = spaceChar & 0xff;
#else
#error "unknown endian"
#endif

    for (storage = 0; storage <= 5; storage++) {
        for (size = 0; size <= storage; size++) {
            for (leftpad = 0; leftpad <= storage-size; leftpad++) {
                rightpad = (storage-size) - leftpad;

                SqlStringBuffer a(storage, size,
                                  leftpad, rightpad,
                                  textChar, spaceChar,
                                  leftbump, rightbump);
                SqlStringBufferUCS2 b(a);
                
                BOOST_CHECK_EQUAL(b.mStorage, storage * 2);
                BOOST_CHECK_EQUAL(b.mSize, size * 2);
                BOOST_CHECK_EQUAL(b.mLeftPad, leftpad * 2);
                BOOST_CHECK_EQUAL(b.mRightPad, rightpad * 2);
                BOOST_CHECK_EQUAL(static_cast<int>(b.mS.size()),
                                  (storage*2)+leftbump+rightbump);
                
                BOOST_CHECK(a.verify());
                BOOST_CHECK(b.verify());

                char *p = b.mLeftP;
                // left bumper
                for (k = 0; k < leftbump; k++) {
                    BOOST_CHECK_EQUAL(*(p++), SqlStringBuffer::mBumperChar);
                }
                BOOST_CHECK(p == b.mStr);
                // left padding
                for (k = 0; k < leftpad; k++) {
                    BOOST_CHECK_EQUAL(*(p++), spaceChar1);
                    BOOST_CHECK_EQUAL(*(p++), spaceChar2);
                }
                // text
                for (k = 0; k < size; k++) {
                    BOOST_CHECK_EQUAL(*(p++), textChar1);
                    BOOST_CHECK_EQUAL(*(p++), textChar2);
                }
                // right padding
                for (k = 0; k < rightpad; k++) {
                    BOOST_CHECK_EQUAL(*(p++), spaceChar1);
                    BOOST_CHECK_EQUAL(*(p++), spaceChar2);
                }
                BOOST_CHECK(p == b.mRightP);
                // right bumper
                for (k = 0; k < rightbump; k++) {
                    BOOST_CHECK_EQUAL(*(p++), SqlStringBuffer::mBumperChar);
                }
                BOOST_CHECK_EQUAL(static_cast<int>(p - b.mLeftP),
                                  storage*2+leftbump+rightbump);
        
                BOOST_CHECK(b.verify());

                p = b.mStr;
                for (k = 0; k < size; k++) {
                    *(p++) = 0x00;
                    *(p++) = '0' + (k % 10);

                }
                BOOST_CHECK(b.verify());

                b.randomize();
                BOOST_CHECK(b.verify());
            }
        }
    }
}

// Test catting 3 fixed width strings together as proof-of-concept
void
SqlStringTest::testSqlStringCat_Fix()
{
    int src1_storage, src2_storage, src3_storage, dst_storage;
    int src1_len, src2_len, src3_len;
    bool caught;
    int newlen;
    
    for (dst_storage = 0; dst_storage < MAXLEN; dst_storage++) {
        for (src1_storage = 0; src1_storage < MAXLEN; src1_storage++) {
            for (src1_len = 0; src1_len <= src1_storage; src1_len++) {
                for (src2_storage = 0; src2_storage < MAXLEN; src2_storage++) {
                    for (src2_len = 0; src2_len <= src2_storage; src2_len++) {
                        for (src3_storage = 0; src3_storage < MAXLEN; src3_storage++) {
                            for (src3_len = 0; src3_len <= src3_storage; src3_len++) {
                                SqlStringBuffer dst(dst_storage, 0,
                                                    0, dst_storage, 
                                                    'd', ' ');
                                SqlStringBuffer src1(src1_storage, src1_len,
                                                     0, src1_storage-src1_len,
                                                     '1', ' ');
                                SqlStringBuffer src2(src2_storage, src2_len,
                                                     0, src2_storage-src2_len,
                                                     '2', ' ');
                                SqlStringBuffer src3(src3_storage, src3_len,
                                                     0, src3_storage-src3_len,
                                                     '3', ' ');
                                
                                caught = false;
                                try {
                                    newlen = SqlStrCat(dst.mStr, dst_storage,
                                                       src1.mStr, src1_storage,
                                                       src2.mStr, src2_storage);
                                } catch (const char *str) {
                                    caught = true;
                                    BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                                    BOOST_CHECK(src1_storage + src2_storage > dst_storage);
                                    BOOST_CHECK(dst.verify());
                                    BOOST_CHECK(src1.verify());
                                    BOOST_CHECK(src2.verify());
                                } catch (...) {
                                    // unexpected exception
                                    BOOST_CHECK(false);
                                }
                                if (!caught) {
                                    BOOST_CHECK(src1_storage + src2_storage <= dst_storage);
                                    BOOST_CHECK(dst.verify());
                                    BOOST_CHECK(src1.verify());
                                    BOOST_CHECK(src2.verify());
                                    
                                    caught = false;
                                    try {
                                        newlen = SqlStrCat(dst.mStr,
                                                           dst_storage,
                                                           newlen,
                                                           src3.mStr,
                                                           src3_storage);
                                    } catch (const char *str) {
                                        caught = true;
                                        BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                                        BOOST_CHECK((src1_storage + 
                                                     src2_storage +
                                                     src3_storage) > dst_storage);
                                        BOOST_CHECK(dst.verify());
                                        BOOST_CHECK(src1.verify());
                                        BOOST_CHECK(src2.verify());
                                        BOOST_CHECK(src3.verify());
                                    } catch (...) {
                                        // unexpected exception
                                        BOOST_CHECK(false);
                                    }
                                    if (!caught) {
                                        BOOST_CHECK(dst.verify());
                                        BOOST_CHECK(src1.verify());
                                        BOOST_CHECK(src2.verify());
                                        BOOST_CHECK(src3.verify());
                                        BOOST_CHECK_EQUAL(newlen, 
                                                          (src1_storage +
                                                           src2_storage +
                                                           src3_storage));

                                        string result(dst.mStr, newlen);
                                        string expect(src1.mStr, src1_storage);
                                        expect.append(src2.mStr, src2_storage);
                                        expect.append(src3.mStr, src3_storage);

                                        BOOST_CHECK(!result.compare(expect));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
}


void
SqlStringTest::testSqlStringCat_Var2()
{
    int src1_storage, src2_storage, dst_storage, src1_len, src2_len;
    int newlen;
    bool caught;
    
    for (dst_storage = 0; dst_storage < MAXLEN; dst_storage++) {
        for (src1_storage = 0; src1_storage < MAXLEN; src1_storage++) {
            for (src1_len = 0; src1_len <= src1_storage; src1_len++) {
                for (src2_storage = 0; src2_storage < MAXLEN; src2_storage++) {
                    for (src2_len = 0; src2_len <= src2_storage; src2_len++) {
                        SqlStringBuffer dst(dst_storage, 0,
                                            0, dst_storage, 
                                            'd', ' ');
                        SqlStringBuffer src1(src1_storage, src1_len,
                                             0, src1_storage-src1_len,
                                             's', ' ');
                        SqlStringBuffer src2(src2_storage, src2_len,
                                             0, src2_storage-src2_len,
                                             'S', ' ');

                        caught = false;
                        try {
                            newlen = SqlStrCat(dst.mStr,
                                               dst_storage,
                                               src1.mStr,
                                               src1_len,
                                               src2.mStr,
                                               src2_len);
                        } catch (const char *str) {
                            caught = true;
                            BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                            BOOST_CHECK(src1_len + src2_len > dst_storage);
                        } catch (...) {
                            // unexpected exception
                            BOOST_CHECK(false);
                        }
                        if (!caught) {
                            BOOST_CHECK(src1_len + src2_len <= dst_storage);
                            BOOST_CHECK_EQUAL(newlen, src1_len + src2_len);

                            string expect;
                            expect.append(src1_len, 's');
                            expect.append(src2_len, 'S');

                            string result(dst.mStr, newlen);

                            BOOST_CHECK(!result.compare(expect));
                            BOOST_CHECK(!expect.compare(result));
                        }
                        BOOST_CHECK(dst.verify());
                        BOOST_CHECK(src1.verify());
                        BOOST_CHECK(src2.verify());
                    }
                }
            }
        }
    }
}


void
SqlStringTest::testSqlStringCat_Var()
{
    int src_storage, dst_storage, src_len, dst_len;
    int newlen;
    bool caught;
    
    for (dst_storage = 0; dst_storage < MAXLEN; dst_storage++) {
        for (dst_len = 0; dst_len <= dst_storage; dst_len++) {
            for (src_storage = 0; src_storage < MAXLEN; src_storage++) {
                for (src_len = 0; src_len <= src_storage; src_len++) {
                    SqlStringBuffer dst(dst_storage, dst_len,
                                        0, dst_storage - dst_len, 
                                        'd', ' ');
                    SqlStringBuffer src(src_storage, src_len,
                                        0, src_storage-src_len,
                                        's', ' ');
                    caught = false;
                    try {
                        newlen = SqlStrCat(dst.mStr, 
                                           dst_storage,
                                           dst_len,
                                           src.mStr,
                                           src_len);
                    } catch (const char *str) {
                        caught = true;
                        BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                        BOOST_CHECK(src_len + dst_len > dst_storage);
                    } catch (...) {
                        // unexpected exception
                        BOOST_CHECK(false);
                    }
                    if (!caught) {
                        BOOST_CHECK(src_len + dst_len <= dst_storage);
                        BOOST_CHECK_EQUAL(newlen, src_len + dst_len);
                    
                        string expect;
                        expect.append(dst_len, 'd');
                        expect.append(src_len, 's');

                        string result(dst.mStr, newlen);

                        BOOST_CHECK(!result.compare(expect));
                        BOOST_CHECK(!expect.compare(result));
                    }
                    BOOST_CHECK(dst.verify());
                    BOOST_CHECK(src.verify());
                }
            }
        }
    }
}
void
SqlStringTest::testSqlStringCpy_Fix()
{
    int src_storage, dst_storage, src_len, dst_len;
    bool caught;
    
    for (dst_storage = 0; dst_storage < MAXLEN; dst_storage++) {
        for (dst_len = 0; dst_len <= dst_storage; dst_len++) {
            for (src_storage = 0; src_storage < MAXLEN; src_storage++) {
                for (src_len = 0; src_len <= src_storage; src_len++) {
                    // ASCII
                    SqlStringBuffer dst(dst_storage, dst_len,
                                        0, dst_storage - dst_len, 
                                        'd', ' ');
                    SqlStringBuffer src(src_storage, src_len,
                                        0, src_storage-src_len,
                                        's', ' ');
                    caught = false;
                    try {
                        SqlStrCpy_Fix<1,1>(dst.mStr, 
                                           dst_storage,
                                           src.mStr,
                                           src_len);
                    } catch (const char *str) {
                        caught = true;
                        BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                        BOOST_CHECK(src_len > dst_storage);
                    } catch (...) {
                        // unexpected exception
                        BOOST_CHECK(false);
                    }
                    if (!caught) {
                        BOOST_CHECK(src_len <= dst_storage);
                        string expect;
                        expect.append(src_len, 's');
                        expect.append(dst_storage - src_len, ' ');
                        //expect.erase(dst_storage);

                        string result(dst.mStr, dst_storage);
#if 0
                        BOOST_MESSAGE(" dst_storage=" << dst_storage <<
                                      " dst_len=" << dst_len <<
                                      " src_storage=" << src_storage <<
                                      " src_len=" << src_len);
                        BOOST_MESSAGE("src =|" << src.mLeftP << "|");
                        BOOST_MESSAGE("expect |" << expect << "|");
                        BOOST_MESSAGE("result |" << result << "|");
#endif
                        BOOST_CHECK(!result.compare(expect));
                    }
                    BOOST_CHECK(dst.verify());
                    BOOST_CHECK(src.verify());

                    // UCS2
                    SqlStringBufferUCS2 srcU2(src);
                    SqlStringBufferUCS2 dstU2(dst);

                    caught = false;
                    try {
                        SqlStrCpy_Fix<2,1>(dstU2.mStr, 
                                           dstU2.mStorage,
                                           srcU2.mStr,
                                           srcU2.mSize);
                    } catch (const char *str) {
                        caught = true;
                        BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                        BOOST_CHECK(srcU2.mSize > dstU2.mStorage);
                    } catch (...) {
                        // unexpected exception
                        BOOST_CHECK(false);
                    }
                    if (!caught) {
                        BOOST_CHECK(srcU2.mSize <= dstU2.mStorage);
                        string expect;
                        BOOST_REQUIRE(!(srcU2.mSize & 1));
                        BOOST_REQUIRE(!(dstU2.mStorage & 1));
                        appendCharsToUCS2LikeString(expect,
                                                    srcU2.mSize >> 1,
                                                    's');
                        appendCharsToUCS2LikeString(expect,
                                                    (dstU2.mStorage -
                                                     srcU2.mSize) >> 1,
                                                    ' ');
                        string result(dstU2.mStr, dstU2.mStorage);
#if 0
                        BOOST_MESSAGE(" dstU2.mStorage=" << dstU2.mStorage <<
                                      " dstU2.mSize=" << dstU2.mSize <<
                                      " srcU2.mStorage=" << srcU2.mStorage <<
                                      " srcU2.mSize=" << srcU2.mSize);
                        BOOST_MESSAGE("srcU2 =|" << srcU2.mLeftP << "|");
                        BOOST_MESSAGE("expectU2 |" << expect << "|");
                        BOOST_MESSAGE("resultU2 |" << result << "|");
#endif
                        BOOST_CHECK(!result.compare(expect));
                    }
                    BOOST_CHECK(dstU2.verify());
                    BOOST_CHECK(srcU2.verify());

                }
            }
        }
    }
}

void
SqlStringTest::testSqlStringCpy_Var()
{
    int src_storage, dst_storage, src_len, dst_len;
    int newlen;
    bool caught;
    
    for (dst_storage = 0; dst_storage < MAXLEN; dst_storage++) {
        for (dst_len = 0; dst_len <= dst_storage; dst_len++) {
            for (src_storage = 0; src_storage < MAXLEN; src_storage++) {
                for (src_len = 0; src_len <= src_storage; src_len++) {
                    // ASCII
                    SqlStringBuffer dst(dst_storage, dst_len,
                                        0, dst_storage - dst_len, 
                                        'd', ' ');
                    SqlStringBuffer src(src_storage, src_len,
                                        0, src_storage-src_len,
                                        's', ' ');
                    caught = false;
                    try {
                        newlen = SqlStrCpy_Var(dst.mStr, 
                                               dst_storage,
                                               src.mStr,
                                               src_len);
                    } catch (const char *str) {
                        caught = true;
                        BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                        BOOST_CHECK(src_len > dst_storage);
                    } catch (...) {
                        // unexpected exception
                        BOOST_CHECK(false);
                    }
                    if (!caught) {
                        BOOST_CHECK(src_len <= dst_storage);
                        BOOST_CHECK_EQUAL(newlen, src_len);
                    
                        string expect;
                        expect.append(src_len, 's');

                        string result(dst.mStr, newlen);

                        BOOST_CHECK(!result.compare(expect));
                        BOOST_CHECK(!expect.compare(result));
                    }
                    BOOST_CHECK(dst.verify());
                    BOOST_CHECK(src.verify());

                    // UCS2 
                    SqlStringBufferUCS2 srcU2(src);
                    SqlStringBufferUCS2 dstU2(dst);
                    caught = false;
                    try {
                        newlen = SqlStrCpy_Var(dstU2.mStr, 
                                               dstU2.mStorage,
                                               srcU2.mStr,
                                               srcU2.mSize);
                    } catch (const char *str) {
                        caught = true;
                        BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                        BOOST_CHECK(srcU2.mSize > dstU2.mStorage);
                    } catch (...) {
                        // unexpected exception
                        BOOST_CHECK(false);
                    }
                    if (!caught) {
                        BOOST_CHECK(srcU2.mSize <= dstU2.mStorage);
                        BOOST_CHECK_EQUAL(newlen, srcU2.mSize);
                    
                        string expect;
                        appendCharsToUCS2LikeString(expect,
                                                    src_len,
                                                    's');
                        string result(dstU2.mStr, newlen);

                        BOOST_CHECK(!result.compare(expect));
                        BOOST_CHECK(!expect.compare(result));
                    }
                    BOOST_CHECK(dstU2.verify());
                    BOOST_CHECK(srcU2.verify());
                    
                }
            }
        }
    }
}


int
SqlStringTest::testSqlStringNormalizeLexicalCmp(int v)
{
    if (v < 0) return -1;
    if (v > 0) return 1;
    return 0;
}

void
SqlStringTest::testSqlStringCmp_Helper(SqlStringBuffer &src1,
                                           int src1_storage,
                                           int src1_len,
                                           SqlStringBuffer &src2,
                                           int src2_storage,
                                           int src2_len)
{
    int result, resultflip;
    
    string s1(src1.mStr, src1_len);
    string s2(src2.mStr, src2_len);
    string s1F(src1.mStr, src1_storage);
    string s2F(src2.mStr, src2_storage);

    // It is possible that test string ends with a space. Remove it.
    s1.erase (s1.find_last_not_of ( " " ) + 1);
    s2.erase (s2.find_last_not_of ( " " ) + 1);
        
    int expected = testSqlStringNormalizeLexicalCmp(s1.compare(s2));
    char const * const s1p = s1.c_str();
    char const * const s2p = s2.c_str();
    int expected2 = testSqlStringNormalizeLexicalCmp(strcmp(s1p, s2p));
    BOOST_CHECK_EQUAL(expected, expected2);

#if 0
    BOOST_MESSAGE("src1=|" << s1 << "|" << 
                  " src2=|" << s2 << "|" <<
                  " src1F=|" << s1F << "|" <<
                  " src2F=|" << s2F << "|" <<
                  " expect=" << expected <<
                  " expect2=" << expected2);
#endif     

    // Test in a more CHAR / padded way
    result = SqlStrCmp<1,1>(src1.mStr, src1_storage,
                            src2.mStr, src2_storage);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(result, expected);

    // Test in a more CHAR / padded way, flip arguments
    resultflip = SqlStrCmp<1,1>(src2.mStr, src2_storage,
                                src1.mStr, src1_storage);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(resultflip * -1, result);

    // Test in a more VARCHAR / non-padded way
    result = SqlStrCmp<1,1>(src1.mStr, src1_len,
                            src2.mStr, src2_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(result, expected);

    // Test in a more VARCHAR / non-padded way, flip arguments
    resultflip = SqlStrCmp<1,1>(src2.mStr, src2_len,
                                src1.mStr, src1_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(resultflip * -1, result);

    // Test in a mixed CHAR/VARCHAR mode
    result = SqlStrCmp<1,1>(src1.mStr, src1_len,
                            src2.mStr, src2_storage);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(result, expected);

    // Test in a mixed CHAR/VARCHAR mode, flip
    resultflip = SqlStrCmp<1,1>(src2.mStr, src2_storage,
                                src1.mStr, src1_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(resultflip * -1, result);

    // Test in a mixed CHAR/VARCHAR mode, flip types
    result = SqlStrCmp<1,1>(src1.mStr, src1_storage,
                            src2.mStr, src2_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(result, expected);

    // Test in a mixed CHAR/VARCHAR mode, flip types and flip args
    resultflip = SqlStrCmp<1,1>(src2.mStr, src2_len,
                                src1.mStr, src1_storage);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(resultflip * -1, result);


    // force check of equal strings
    result = SqlStrCmp<1,1>(src1.mStr, src1_storage,
                            src1.mStr, src1_storage);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK_EQUAL(result, 0);

    result = SqlStrCmp<1,1>(src1.mStr, src1_len,
                            src1.mStr, src1_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK_EQUAL(result, 0);

    result = SqlStrCmp<1,1>(src1.mStr, src1_storage,
                            src1.mStr, src1_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK_EQUAL(result, 0);

    result = SqlStrCmp<1,1>(src1.mStr, src1_len,
                            src1.mStr, src1_storage);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK_EQUAL(result, 0);

    result = SqlStrCmp<1,1>(src2.mStr, src2_storage,
                            src2.mStr, src2_storage);
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(result, 0);

    result = SqlStrCmp<1,1>(src2.mStr, src2_len,
                            src2.mStr, src2_len);
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(result, 0);
}


void
SqlStringTest::testSqlStringCmp()
{
    int src1_storage, src2_storage, src1_len, src2_len;
    
    // can't test w/ 0, confuses strcmp and/or std:string
    // want to test test some values less than 'space', some 
    // values between 'space' and 127, some values around 127,
    // and some values above 127. Thus test every 30
    // characters (space is 32) in the full 8-bit range to
    // hit all combinations these areas to insure that there
    // are no unsigned/signed issues, less than space
    // issues, etc. given that PAD SPACE is the default and
    // only supported mode, testing characters less than
    // space doesn't make sense until NO PAD is supported.
    // therefore startchar, for now starts one greater than
    // space
    int startc = ' ' + 1;  // should be 1 to test NO PAD
    int range = 255 - startc;

    for (src1_storage = 0; src1_storage <= MAXLEN; src1_storage++) {
        for (src1_len = 0; src1_len < src1_storage; src1_len++) {
            for (src2_storage = 0; src2_storage <= MAXLEN; src2_storage++) {
                for (src2_len = 0; src2_len < src2_storage; src2_len++) {
                    for (int startchar1 = startc; startchar1 < 255;
                         startchar1+=30) {
                        for (int startchar2 = startc; startchar2 < 255;
                             startchar2+=30) {
                            SqlStringBuffer src1(src1_storage, src1_len,
                                                 0, src1_storage - src1_len, 
                                                 'd', ' ');
                            SqlStringBuffer src2(src2_storage, src2_len,
                                                 0, src2_storage-src2_len,
                                                 's', ' ');
                            
                            src1.patternfill(startchar1, startc, 255);
                            src2.patternfill(startchar2, startc, 255);
                            
                            testSqlStringCmp_Helper(src1, src1_storage,
                                                    src1_len,
                                                    src2, src2_storage,
                                                    src2_len);
                        }
                    }
                }
                // try some fully random character groupings as a test
                // to the more controlled test above
                for (int randX = 0; randX < 5; randX++) {
                    SqlStringBuffer src1(src1_storage, src1_len,
                                         0, src1_storage - src1_len, 
                                         'd', ' ');
                    SqlStringBuffer src2(src2_storage, src2_len,
                                         0, src2_storage-src2_len,
                                         's', ' ');
                    
                    src1.randomize(startc + (rand() % range), startc, 255);
                    src2.randomize(startc + (rand() % range), startc, 255);
                    
                    testSqlStringCmp_Helper(src1, src1_storage, src1_len,
                                            src2, src2_storage, src2_len);
                }
            }
        }
    }
}

// src1 and src2 are always not equal, except when both 0 length
void
SqlStringTest::testSqlStringCmp_Bin_Helper(
    SqlStringBuffer &src1,
    int src1_storage,
    int src1_len,
    SqlStringBuffer &src2,
    int src2_storage,
    int src2_len)
{
    int result;
    string s1(src1.mStr, src1_len);
    string s2(src2.mStr, src2_len);
#if 0
    BOOST_MESSAGE("src1=|" << s1 << "| " << src1_len << " " << src1_storage <<
                  " src2=|" << s2 << "| " << src2_len << " " << src2_storage);
#endif     
 
    int expected = testSqlStringNormalizeLexicalCmp(s1.compare(s2));
    
    // (generally) different values
    result = SqlStrCmp_Bin(src1.mStr, src1_len,
                           src2.mStr, src2_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(expected, result);

    // swap different values
    result = SqlStrCmp_Bin(src2.mStr, src2_len,
                           src1.mStr, src1_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(expected * -1, result);
        
    // same string
    result = SqlStrCmp_Bin(src1.mStr, src1_len,
                           src1.mStr, src1_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK_EQUAL(0, result);

    // same string
    result = SqlStrCmp_Bin(src2.mStr, src2_len,
                           src2.mStr, src2_len);
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(0, result);
}

// Lower must be > 0 to allow the use of strncmp() in testing
void
SqlStringTest::testSqlStringCmp_Bin_Helper2(int &lower, int &upper)
{
    lower = (rand() % 254) + 1;
    upper = lower + (rand() % (255-lower));
    assert(lower > 0 && lower < 256);
    assert(upper > 0 && upper < 256);
    assert(lower <= upper);
}

void
SqlStringTest::testSqlStringCmp_Bin()
{
    // See SQL2003 Part 2 Section 4.3.2. Binary strings are equal only if
    // they are same length if following SQL2003 strictly. Allow an
    // extension to test for inequalities, therefore test for -1, 0, 1
    // memcmp() semantics.
    int src1_storage, src2_storage, src1_len, src2_len;

    for (src1_storage = 0; src1_storage <= MAXCMPLEN; src1_storage++) {
        src1_len = src1_storage;
        for (src2_storage = 0; src2_storage <= MAXCMPLEN; src2_storage++) {
            src2_len = src2_storage;
            SqlStringBuffer src1(src1_storage, src1_len,
                                 0, src1_storage - src1_len, 
                                 'd', ' ');
            SqlStringBuffer src2(src2_storage, src2_len,
                                 0, src2_storage-src2_len,
                                 's', ' ');
            testSqlStringCmp_Bin_Helper(
                src1, src1_storage, src1_len,
                src2, src2_storage, src2_len);

            if (src1_len == 0 || src2_len == 0) {
                continue;
            }
            int lower, upper;
            int maxcmp = MAXCMPRANDOM >> 2; // no need for high iteration count
            if (maxcmp < 16) maxcmp = 16;
            for (int randX = 0; randX < maxcmp; randX++) {
                testSqlStringCmp_Bin_Helper2(lower, upper);
                src1.randomize(lower, lower, upper);
                // src2 must not == src1, except when 0 length
                int count = 100;
                do {
                    testSqlStringCmp_Bin_Helper2(lower, upper);
                    src2.randomize(lower, lower, upper);
                } while (src1_len > 0 && count-- > 0 &&
                         !memcmp(src1.mStr, src2.mStr, src1_len));
                if (count < 1) {
                    // bad luck, just give up on this iteration
                    BOOST_MESSAGE("giving up on impossible random string gen");
                    break;
                }
                testSqlStringCmp_Bin_Helper(
                    src1, src1_storage, src1_len,
                    src2, src2_storage, src2_len);
            }
        }
    }
}

void
SqlStringTest::testSqlStringLenBit()
{
    int src_storage, src_len;
    int newlen;
    
    src_storage = MAXLEN;
    for (src_storage = 0; src_storage <= MAXLEN; src_storage++) {
        for (src_len = 0; src_len <= src_storage; src_len++) {
            SqlStringBuffer src(src_storage, src_len,
                                0, src_storage-src_len,
                                's', ' ');

            // VARCHAR-ish test
            newlen = SqlStrLenBit(src_len);
            BOOST_CHECK_EQUAL(newlen, src_len * 8);
            BOOST_CHECK(src.verify());

            // CHAR-ish test
            newlen = SqlStrLenBit(src_storage);
            BOOST_CHECK_EQUAL(newlen, src_storage * 8);
            BOOST_CHECK(src.verify());

            SqlStringBufferUCS2 srcU2(src);
            
            // VARCHAR-ish test
            newlen = SqlStrLenBit(srcU2.mSize);
            BOOST_CHECK_EQUAL(newlen, srcU2.mSize * 8);
            BOOST_CHECK(src.verify());

            // CHAR-ish test
            newlen = SqlStrLenBit(srcU2.mStorage);
            BOOST_CHECK_EQUAL(newlen, srcU2.mStorage * 8);
            BOOST_CHECK(src.verify());

        }
    }
}

void
SqlStringTest::testSqlStringLenChar()
{
    int src_storage, src_len;
    int newlen;
    
    src_storage = MAXLEN;
    for (src_storage = 0; src_storage <= MAXLEN; src_storage++) {
        for (src_len = 0; src_len <= src_storage; src_len++) {
            SqlStringBuffer src(src_storage, src_len,
                                0, src_storage-src_len,
                                's', ' ');

            // VARCHAR-ish test
            newlen = SqlStrLenChar<1,1>(src.mStr,
                                        src_len);
            BOOST_CHECK_EQUAL(newlen, src_len);
            BOOST_CHECK(src.verify());

            // CHAR-ish test
            newlen = SqlStrLenChar<1,1>(src.mStr,
                                        src_storage);
            BOOST_CHECK_EQUAL(newlen, src_storage);
            BOOST_CHECK(src.verify());

            SqlStringBufferUCS2 srcU2(src);

            // VARCHAR-ish test
            newlen = SqlStrLenChar<2,1>(srcU2.mStr,
                                        srcU2.mSize);
            // the number characters is unchanged from Ascii src / src_len
            BOOST_CHECK_EQUAL(newlen, src_len);
            BOOST_CHECK(src.verify());

            // CHAR-ish test
            newlen = SqlStrLenChar<2,1>(srcU2.mStr,
                                        srcU2.mStorage);
            // the number characters is unchanged from Ascii src / src_storage
            BOOST_CHECK_EQUAL(newlen, src_storage);
            BOOST_CHECK(src.verify());
        }
    }
}


void
SqlStringTest::testSqlStringLenOct()
{
    int src_storage, src_len;
    int newlen;
    
    src_storage = MAXLEN;
    for (src_storage = 0; src_storage <= MAXLEN; src_storage++) {
        for (src_len = 0; src_len <= src_storage; src_len++) {
            SqlStringBuffer src(src_storage, src_len,
                                0, src_storage-src_len,
                                's', ' ');

            // VARCHAR-ish test
            newlen = SqlStrLenOct(src_len);
            BOOST_CHECK_EQUAL(newlen, src_len);
            BOOST_CHECK(src.verify());

            // CHAR-ish test
            newlen = SqlStrLenOct(src_storage);
            BOOST_CHECK_EQUAL(newlen, src_storage);
            BOOST_CHECK(src.verify());

            SqlStringBufferUCS2 srcU2(src);

            // VARCHAR-ish test
            newlen = SqlStrLenOct(srcU2.mSize);
            BOOST_CHECK_EQUAL(newlen, srcU2.mSize);
            BOOST_CHECK(srcU2.verify());

            // CHAR-ish test
            newlen = SqlStrLenOct(srcU2.mStorage);
            BOOST_CHECK_EQUAL(newlen, srcU2.mStorage);
            BOOST_CHECK(srcU2.verify());
        }
    }
}


void
SqlStringTest::testSqlStringOverlay()
{
    int dst_storage, src_storage, src_len, over_storage, over_len;
    int position, length, lengthI;
    int exLeftLen, exMidLen, exRightLen;
    char *exLeftP, *exMidP, *exRightP;
    bool lenSpecified;
    bool caught = false;
    int newlen = 0;

    for (dst_storage = 0; dst_storage < MAXLEN; dst_storage++) {
        for (src_storage = 0; src_storage < MAXLEN; src_storage++) {
            src_len = src_storage;
            for (over_storage = 0; over_storage < MAXLEN; over_storage++) {
                over_len = over_storage;
                for (position = 0; position < MAXLEN; position++) {
                    for (lengthI = -1; lengthI < MAXLEN; lengthI++) {
                        if (lengthI == -1) {
                            lenSpecified = false;
                            length = over_len;
                        } else {
                            lenSpecified = true;
                            length = lengthI;
                        }
#if 0
                        BOOST_MESSAGE(" dst_storage=" << dst_storage <<
                                      " src_storage=" << src_storage <<
                                      " over_storage=" << over_storage <<
                                      " pos=" << position <<
                                      " length=" << length <<
                                      " spec=" << lenSpecified);
#endif                        
                        SqlStringBuffer dst(dst_storage, dst_storage,
                                            0, 0,
                                            'd', ' ');
                        SqlStringBuffer src(src_storage, src_len,
                                            0, src_storage-src_len,
                                            's', ' ');
                        SqlStringBuffer over(over_storage, over_len,
                                             0, over_storage-over_len,
                                             'o', ' ');

                        src.patternfill('a', 'a', 'z');
                        over.patternfill('A', 'A', 'Z');

                        // ex* vars are 0-indexed. for loops are 1-indexed
                        exLeftP = src.mStr;
                        if (position >= 1 && src_len >= 1) {
                            exLeftLen = position - 1;  // 1-idx -> 0-idx
                            if (exLeftLen > src_len) exLeftLen = src_len;
                        } else {
                            exLeftLen = 0;
                        }

                        exMidP = over.mStr;
                        exMidLen = over_len;
                        
                        exRightLen = src_len - (exLeftLen + length);
                        if (exRightLen < 0) exRightLen = 0;
                        exRightP = exLeftP + (src_len - exRightLen);

                        string expect(exLeftP, exLeftLen);
                        expect.append(exMidP, exMidLen);
                        expect.append(exRightP, exRightLen);

                        caught = false;
                        try {
                            newlen = SqlStrOverlay<1,1>(dst.mStr, 
                                                        dst_storage,
                                                        src.mStr,
                                                        src_len,
                                                        over.mStr,
                                                        over_len,
                                                        position,
                                                        length,
                                                        lenSpecified);
                        } catch (const char *str) {
                            caught = true;
                            if (!strcmp(str, "22011")) {
                                BOOST_CHECK(position < 1 || (lenSpecified && length < 1));
                            } else if (!strcmp(str, "22001")) {
                                BOOST_CHECK(src_len + over_len > dst_storage);
                            } else {
                                BOOST_CHECK(false);
                            }
                            
                        }
                        if (!caught) {
                            BOOST_CHECK(position > 0);
                            if (lenSpecified) {
                                BOOST_CHECK(length >= 0);
                            }
                    
                            BOOST_CHECK(dst.verify());
                            BOOST_CHECK(src.verify());
                            BOOST_CHECK(over.verify());

                            string result(dst.mStr, newlen);

                            BOOST_CHECK(!result.compare(expect));
                            BOOST_CHECK(!expect.compare(result));
                        }
                    }
                }
            }
        }
    }
}

void
SqlStringTest::testSqlStringPos()
{
    int src_storage, find_start, find_len, randX;
    int alter_char;
    
    int foundpos;
    
    for (src_storage = 0; src_storage < MAXLEN; src_storage++) {
        for (randX = 0; randX < MAXRANDOM; randX++) {

            SqlStringBuffer src(src_storage, src_storage,
                                0, 0,
                                's', ' ');
            src.randomize('a', 'a', 'z');
        
            // find all possible valid substrings
            for (find_start = 0; find_start <= src_storage; find_start++) {
                for (find_len = 0; find_len <= src_storage - find_start; find_len++) {
                    string validsubstr(src.mStr + find_start, find_len);
                    SqlStringBuffer find(find_len, find_len, 
                                         0, 0,
                                         'X', ' ');
                    memcpy(find.mStr, validsubstr.c_str(), find_len);
                
                    foundpos  = SqlStrPos<1,1>(src.mStr, 
                                               src_storage,
                                               find.mStr,
                                               find_len);
                    BOOST_CHECK(src.verify());
                    BOOST_CHECK(find.verify());

                    if (find_len) {
                        // foundpos is 1-indexed. find_start is 0-indexed.
                        BOOST_CHECK_EQUAL(foundpos, find_start + 1);
                    } else {
                        BOOST_CHECK_EQUAL(foundpos, static_cast<int>(1));  // Case A.
                    
                    }

                    // alter valid substring to prevent match
                    for (alter_char = 0; alter_char < find_len; alter_char++) {
                        char save = *(find.mStr + alter_char);
                        *(find.mStr + alter_char) = 'X'; // 'X' not between 'a' and 'z'

                        foundpos  = SqlStrPos<1,1>(src.mStr, 
                                                   src_storage,
                                                   find.mStr,
                                                   find_len);
                        BOOST_CHECK(src.verify());
                        BOOST_CHECK(find.verify());

                        BOOST_CHECK_EQUAL(foundpos, static_cast<int>(0));
                    
                        *(find.mStr + alter_char) = save;
                    }
                }
            }
        }
    }
}

void
SqlStringTest::testSqlStringSubStr()
{
    int src_storage, src_len, dst_storage, newlen = 0;
    int sub_start, sub_len;
    bool caught;
    char const * resultP;

    // must test where substart and/or sublen larger than src_storage and
    // less than 0
    
    for (dst_storage = 0; dst_storage < MAXLEN; dst_storage++) {
        for (src_storage = 0; src_storage <= dst_storage; src_storage++) {
            for (src_len = 0; src_len <= src_storage; src_len++) {
                for (sub_start = -3; sub_start <= 3 + src_storage; sub_start++) {
                    for (sub_len = -3; sub_len <= 3 + src_storage; sub_len++) {
                        SqlStringBuffer dst(dst_storage, dst_storage,
                                            0, 0,
                                            'd', ' ');
                        SqlStringBuffer src(src_storage, src_len,
                                            0, src_storage - src_len,
                                            's', ' ');
                        src.randomize();
#if 0
                        BOOST_MESSAGE("src =|" << src.mLeftP <<
                                      "| dst_storage=" << dst_storage <<
                                      " src_storage=" << src_storage << 
                                      " src_len=" << src_len << 
                                      " sub_start=" << sub_start <<
                                      " sub_len=" << sub_len);
#endif
                        int exsubstart = sub_start;
                        int exlen = sub_len;
                        if (exsubstart < 1) {
                            exlen += (exsubstart - 1);      // will grab fewer characters
                        }
                        exsubstart--;                       // convert index
                        if (exsubstart < 0) exsubstart = 0; // clean up for std::string
                        if (exlen < 0) exlen = 0;           // clean up for std::string

                        if (exsubstart + exlen > src_storage) {
                            if (exsubstart > src_storage) {
                                exlen = 0;
                            } else {
                                exlen = src_storage - exsubstart;
                            }
                        }
                        if (exsubstart < 0) exsubstart = 0; // clean up for std::string
                        if (exlen < 0) exlen = 0;           // clean up for std::string

                        string expect(src.mStr + exsubstart, exlen);

                        caught = false;
                        try {
                            newlen = SqlStrSubStr<1,1>(&resultP, dst_storage,
                                                       src.mStr, src_storage,
                                                       sub_start, sub_len, true);
                        } catch (const char *str) {
                            caught = true;
                            if (!strcmp(str, "22011")) {
                                BOOST_CHECK(sub_len < 0);
                            } else if (!strcmp(str, "22001")) {
                                BOOST_CHECK(sub_len > dst_storage);
                                BOOST_CHECK(sub_len >= 0);
                            } else {
                                BOOST_CHECK(false);
                            }
                        }
                        if (!caught) {
                            BOOST_CHECK(sub_len >= 0);
                            string result(resultP, newlen);
#if 0
                            BOOST_MESSAGE("expect |" << expect << "|");
                            BOOST_MESSAGE("result |" << result << "|");
#endif
                            BOOST_CHECK(!result.compare(expect));
                            BOOST_CHECK(!expect.compare(result));
                        }
                        BOOST_CHECK(dst.verify());
                        BOOST_CHECK(src.verify());

                        // length unspecified mode
                        // test when length is at or past the storage
                        if (sub_start > 0 && sub_len > 0 &&
                            sub_start + sub_len - 1 > src_storage) {
                            caught = false;
                            try {
                                newlen = SqlStrSubStr<1,1>(&resultP, dst_storage,
                                                           src.mStr, src_storage,
                                                           sub_start, 0, false);
                            } catch (const char *str) {
                                caught = true;
                                if (!strcmp(str, "22011")) {
                                    BOOST_CHECK(sub_len < 0);
                                } else if (!strcmp(str, "22001")) {
                                    BOOST_CHECK(sub_len > dst_storage);
                                } else {
                                    BOOST_CHECK(false);
                                }
                            }
                            if (!caught) {
                                //                        BOOST_MESSAGE(" len=" << sub_len <<                                     " start=" << sub_start);

                                string result(resultP, newlen);
#if 0
                                BOOST_MESSAGE("expect |" << expect << "|");
                                BOOST_MESSAGE("result |" << result << "|");
#endif
                                BOOST_CHECK(!result.compare(expect));
                                BOOST_CHECK(!expect.compare(result));
                            }
                            BOOST_CHECK(dst.verify());
                            BOOST_CHECK(src.verify());
                        }
                    }
                }
            }
        }
    }
}

// helper to testSqlStringAlterCase()
void
SqlStringTest::testSqlStringAlterCase_Ascii(int dst_storage,
                                            int src_len,
                                            SqlStringBuffer& dest,
                                            SqlStringBuffer& src,
                                            const string& expect, 
                                            SqlStrAlterCaseAction action)
{
    int newlen = 0;
    bool caught = false;

    try {
        switch (action) {
        case AlterCaseUpper:
            newlen = SqlStrAlterCase<1,1,AlterCaseUpper>
                (dest.mStr,
                 dest.mStorage,
                 src.mStr,
                 src.mSize);
            break;
        case AlterCaseLower:
            newlen = SqlStrAlterCase<1,1,AlterCaseLower>
                (dest.mStr,
                 dest.mStorage,
                 src.mStr,
                 src.mSize);
            break;
        default:
            BOOST_REQUIRE(0);
            break;
        }        
    } catch (const char *str) {
        caught = true;
        BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
        BOOST_CHECK(src_len > dst_storage);
    } catch (...) {
        // unexpected exception
        BOOST_CHECK(false);
    }
    if (!caught) {
        BOOST_CHECK(src_len <= dst_storage);
        BOOST_CHECK(src.verify());
        BOOST_CHECK(dest.verify());
        BOOST_CHECK_EQUAL(newlen, src_len);

        string result(dest.mStr, newlen);
#if 0
        BOOST_MESSAGE(" action=" << action << 
                      " newlen="<< newlen <<
                      " result=|" << result << "|" <<
                      " expect=|" << expect << "|");
#endif                    
        BOOST_CHECK(!expect.compare(result));
    }
}

// helper to testSqlStringAlterCase()
void
SqlStringTest::testSqlStringAlterCase_UCS2(int dst_storage,
                                           int src_len,
                                           SqlStringBufferUCS2& destU2,
                                           SqlStringBufferUCS2& srcU2,
                                           const string& expect,
                                           SqlStrAlterCaseAction action)
{
#ifdef HAVE_ICU
    UnicodeString expectU2(expect.c_str(), "iso-8859-1");
    BOOST_REQUIRE(!expectU2.isBogus());

    BOOST_CHECK(srcU2.verify());
    BOOST_CHECK(destU2.verify());

    BOOST_REQUIRE(srcU2.mSize == src_len * 2);
    BOOST_REQUIRE(destU2.mStorage == dst_storage * 2);
         
    int newlen;
    bool caught = false;
    try {
        switch (action) {
        case AlterCaseUpper:
            newlen = SqlStrAlterCase<2,1,AlterCaseUpper>
                (destU2.mStr,
                 destU2.mStorage,
                 srcU2.mStr,
                 srcU2.mSize,
                 ULOC_US);
            break;
        case AlterCaseLower:
            newlen = SqlStrAlterCase<2,1,AlterCaseLower>
                (destU2.mStr,
                 destU2.mStorage,
                 srcU2.mStr,
                 srcU2.mSize,
                 ULOC_US);
            break;
        default:
            BOOST_REQUIRE(0);
            break;
        }
        
    } catch (const char *str) {
        caught = true;
        BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
        BOOST_CHECK(src_len > dst_storage);
    } catch (...) {
        // unexpected exception
        BOOST_CHECK(false);
    }
    if (!caught) {
        BOOST_CHECK(src_len <= dst_storage);
        BOOST_CHECK(srcU2.verify());
        BOOST_CHECK(destU2.verify());
        BOOST_CHECK_EQUAL(newlen, srcU2.mSize);

        UnicodeString
            resultU2(reinterpret_cast<UChar *>(destU2.mStr),
                     newlen >> 1);

        BOOST_REQUIRE(!resultU2.isBogus());
#if 0
        BOOST_MESSAGE("newlen=" << newlen);
        BOOST_MESSAGE("srcU2= |" << srcU2.dump());
        BOOST_MESSAGE("destU2= |" << destU2.dump());
        BOOST_MESSAGE("expect=|" << UnicodeToPrintable(expectU2) << "|");
        BOOST_MESSAGE("result=|" << UnicodeToPrintable(resultU2) << "|");
#endif                    
        BOOST_CHECK(!expectU2.compare(resultU2));
    }
#endif    
}

// helper to testSqlStringAlterCase()
void
SqlStringTest::testSqlStringAlterCase_Case(SqlStrAlterCaseAction action,
                                           int dst_storage,
                                           int dest_len,
                                           int src_storage,
                                           int src_len)
{
    BOOST_REQUIRE(action == AlterCaseUpper || action == AlterCaseLower);

    SqlStringBuffer dest(dst_storage, dest_len,
                         0, 0,
                         'd', ' ');
    SqlStringBuffer src(src_storage, src_len,
                        0, src_storage-src_len,
                        's', ' ');
    string expect;

    switch (action) {
    case AlterCaseUpper:
        src.randomize('a');
        expect.assign(src.mStr, src_len);
        std::transform(expect.begin(), expect.end(),
                       expect.begin(), (int(*)(int)) std::toupper);
        break;
    case AlterCaseLower:
        src.randomize('A');
        expect.assign(src.mStr, src_len);
        std::transform(expect.begin(), expect.end(),
                       expect.begin(), (int(*)(int)) std::tolower);
        break;
    default:
        BOOST_REQUIRE(0);
        break;
    }

    // Ascii
    testSqlStringAlterCase_Ascii(dst_storage, src_len,
                                 dest,
                                 src,
                                 expect,
                                 action);
                
    // UCS2 long-word aligned case
    // perhaps nits are being picked too fine here. could deprecate.
        
    SqlStringBufferUCS2 srcU2LongAligned(src, 4, 4);
    SqlStringBufferUCS2 destU2LongAligned(dest, 4, 4);

    testSqlStringAlterCase_UCS2(dst_storage,
                                src_len,
                                destU2LongAligned,
                                srcU2LongAligned,
                                expect,
                                action);

    // UCS2 short-word aligned case
    SqlStringBufferUCS2 srcU2ShortAligned(src);
    SqlStringBufferUCS2 destU2ShortAligned(dest);

    testSqlStringAlterCase_UCS2(dst_storage,
                                src_len,
                                destU2ShortAligned,
                                srcU2ShortAligned,
                                expect,
                                action);

}


void
SqlStringTest::testSqlStringAlterCase()
{
    int dst_storage, dest_len, src_storage, src_len, randX;

    for (dst_storage = 0; dst_storage < MAXLEN; dst_storage++) {
        dest_len = dst_storage;
        for (src_storage = 0; src_storage < MAXLEN; src_storage++) {
            src_len = src_storage;
            for (randX = 0; randX < MAXRANDOM; randX++) {
                testSqlStringAlterCase_Case(AlterCaseUpper,
                                            dst_storage,
                                            dest_len,
                                            src_storage,
                                            src_len);
                testSqlStringAlterCase_Case(AlterCaseLower,
                                            dst_storage,
                                            dest_len,
                                            src_storage,
                                            src_len);
            }
        }
    }
}

void
SqlStringTest::testSqlStringTrim_Helper(int dst_storage,
                                        int src_storage,
                                        int src_len,
                                        int leftpad,
                                        int rightpad,
                                        int action)
{
    int expectsize, expectsizeU2;
    string expect, expectU2;
    int lefttrim = 0, righttrim = 0;
    char padchar = ' ';
    char textchar = 's';
    bool caught;
    
    switch(action) {
    case 0:
        lefttrim = 1;
        righttrim = 1;
        expect.append(src_len, textchar);
        expectsize = src_len;
        appendCharsToUCS2LikeString(expectU2,
                                    src_len,
                                    textchar);
        break;
    case 1:
        lefttrim = 1;
        righttrim = 0;
        expect.append(src_len, textchar);
        appendCharsToUCS2LikeString(expectU2,
                                    src_len,
                                    textchar);
        // if no text, everything is trimmed
        if (src_len) {
            expect.append(rightpad, padchar);
            appendCharsToUCS2LikeString(expectU2,
                                        rightpad,
                                        padchar);
        }
        expectsize = src_len + (src_len ? rightpad : 0);
        break;
    case 2:
        lefttrim = 0;
        righttrim = 1;
        // if no text, everything is trimmed
        if (src_len) {
            expect.append(leftpad, padchar);
            appendCharsToUCS2LikeString(expectU2,
                                        leftpad,
                                        padchar);
        }
        expect.append(src_len, textchar);
        appendCharsToUCS2LikeString(expectU2,
                                    src_len,
                                    textchar);
        expectsize = src_len + (src_len ? leftpad : 0);
        break;
    case 3:
        lefttrim = 0;
        righttrim = 0;
        expect.append(leftpad, padchar);
        expect.append(src_len, textchar);
        expect.append(rightpad, padchar);
        appendCharsToUCS2LikeString(expectU2,
                                    leftpad,
                                    padchar);
        appendCharsToUCS2LikeString(expectU2,
                                    src_len,
                                    textchar);
        appendCharsToUCS2LikeString(expectU2,
                                    rightpad,
                                    padchar);
        expectsize = src_len + leftpad + rightpad;
        break;
    }
    expectsizeU2 = expectsize * 2;
    BOOST_REQUIRE(expect.length() == expectsize);
    BOOST_REQUIRE(expectU2.length() == expectsizeU2);

    string resultByReference;
    string resultCopy;
    int newsize;
    SqlStringBuffer src(src_storage, src_len,
                        leftpad, rightpad,
                        textchar, padchar);
    SqlStringBuffer dst(dst_storage, dst_storage,
                        0, 0,
                        textchar, padchar);
                            
    // ASCII copy
    caught = false;
    try {
        newsize = 
            SqlStrTrim<1,1>(dst.mStr,
                            dst_storage,
                            src.mStr,
                            src_len + leftpad + rightpad,
                            lefttrim,
                            righttrim);
    } catch (const char *str) {
        caught = true;
        BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
        BOOST_CHECK(expectsize > dst_storage);
    } catch (...) {
        // unexpected exception
        BOOST_CHECK(false);
    }
                            
    if (!caught) {
        BOOST_CHECK(expectsize <= dst_storage);

        BOOST_CHECK(src.verify());
        BOOST_CHECK(dst.verify());
        BOOST_CHECK_EQUAL(newsize, expectsize);
        resultCopy = string(dst.mStr, newsize);

        BOOST_CHECK(!resultCopy.compare(expect));
    }

    // UCS2 copy 
    SqlStringBufferUCS2 srcU2(src);
    SqlStringBufferUCS2 dstU2(dst);
    caught = false;
    try {
        newsize = 
            SqlStrTrim<2,1>(dstU2.mStr,
                            dstU2.mStorage,
                            srcU2.mStr,
                            (srcU2.mSize + 
                             srcU2.mLeftPad +
                             srcU2.mRightPad),
                            lefttrim,
                            righttrim);
    } catch (const char *str) {
        caught = true;
        BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
        BOOST_CHECK(expectsize > dst_storage);
    } catch (...) {
        // unexpected exception
        BOOST_CHECK(false);
    }
                            
    if (!caught) {
        BOOST_CHECK(expectsize <= dst_storage);

        BOOST_CHECK(srcU2.verify());
        BOOST_CHECK(dstU2.verify());
        BOOST_CHECK_EQUAL(newsize, expectsizeU2);
        resultCopy = string(dstU2.mStr, newsize);

        BOOST_CHECK(!resultCopy.compare(expectU2));
    }
                            
                            

    // ASCII by reference
    char const * start;
    newsize = SqlStrTrim<1,1>(&start,
                              src.mStr,
                              src_len + leftpad + rightpad,
                              lefttrim,
                              righttrim);

    BOOST_CHECK(start >= src.mStr);
    BOOST_CHECK(start <= src.mStr + src_storage);
    BOOST_CHECK(src.verify());
    BOOST_CHECK(dst.verify());
    BOOST_CHECK_EQUAL(newsize, expectsize);
    resultByReference = string(start, newsize);
                            
    BOOST_CHECK(!resultByReference.compare(expect));
    BOOST_CHECK(!expect.compare(resultByReference));

    // UCS2 by reference
    newsize = SqlStrTrim<2,1>(&start,
                              srcU2.mStr,
                              (srcU2.mSize + 
                               srcU2.mLeftPad +
                               srcU2.mRightPad),
                              lefttrim,
                              righttrim);

    BOOST_CHECK(start >= srcU2.mStr);
    BOOST_CHECK(start <= srcU2.mStr + srcU2.mStorage);
    BOOST_CHECK(srcU2.verify());
    BOOST_CHECK(dstU2.verify());
    BOOST_CHECK_EQUAL(newsize, expectsizeU2);
    resultByReference = string(start, newsize);
                            
    BOOST_CHECK(!resultByReference.compare(expectU2));
}


void
SqlStringTest::testSqlStringTrim()
{
    int dst_storage, src_storage, src_len, leftpad, rightpad, randX, action;
    
    BOOST_REQUIRE(MAXLEN >= 5); // 2 pad + 1 text + 2 pad

    for (dst_storage = 0; dst_storage < MAXLEN; dst_storage++) {
        for (src_storage = 0; src_storage < dst_storage + 5; src_storage++) {
            for (src_len = 0; src_len <= src_storage; src_len++) {
                for (leftpad = 0; leftpad <= src_storage - src_len; leftpad++) {
                    rightpad = src_storage - (src_len + leftpad);
                    for (randX = 0; randX < MAXRANDOM; randX++) {
                        BOOST_REQUIRE(leftpad+rightpad+src_len == src_storage);
                        for (action = 0; action < 4; action++) {
                            testSqlStringTrim_Helper(dst_storage,
                                                     src_storage,
                                                     src_len,
                                                     leftpad,
                                                     rightpad,
                                                     action);
                        }
                    }
                }
            }
        }
    }
}

void
SqlStringTest::testSqlStringCastToExact_Helper(uint64_t value,
                                               char const * const buf,
                                               int src_storage,
                                               int src_len,
                                               bool exceptionExpected)
{
    bool caught = false;
    int64_t newvalue;
    SqlStringBuffer src(src_storage, src_len,
                        0, src_storage-src_len,
                        's', ' ');

#if 0
    BOOST_MESSAGE("buf = |" << buf << "|");
#endif
    if (strlen(buf) > src_len) {
        // not all test cases will fit, just silently ignore them
        return;
    }

    // copy string, minus null
    memcpy(src.mStr, buf, strlen(buf));
    // pad out any leftovers with spaces (say, if value is very small for 
    // string length)
    memset(src.mStr + strlen(buf), ' ', src_len - strlen(buf));
#if 0
    BOOST_MESSAGE("str = |" << src.mLeftP << "|");
#endif
    
    try {
        newvalue = SqlStrCastToExact<1,1>(src.mStr,
                                          src_len);
    } catch (const char *str) {
        caught = true;
        BOOST_CHECK_EQUAL(strcmp(str, "22018"), 0);
    }
    BOOST_CHECK_EQUAL(caught, exceptionExpected);
    if (!caught) {
        BOOST_CHECK_EQUAL(value, newvalue);
        BOOST_CHECK(src.verify());
    }
}

// tests varchar case, at least partially, when src_len == src_storage
void
SqlStringTest::testSqlStringCastToExact()
{
    int src_storage, src_len;
    int rand_idx;
    int64_t power, poweridx;
    int64_t value, valuer1, valuer2, valuer3;
    char buf[256];
    
    src_storage = MAXLEN;
    //    strlen(2^64) = 20;
    for (src_storage = 1; src_storage <= 20; src_storage++) {
        for (src_len = 1; src_len <= src_storage; src_len++) {
            power = 1;
            for (poweridx = 0; poweridx < src_len; poweridx++) {
                power *= 10;
            }
            // do a bit more than typical random to get decent coverage
            // on positives, negatives, and various length numbers.
            // besides, test runs very quickly anyway.
            for (rand_idx = 0; rand_idx < 5 * MAXRANDOM; rand_idx++) {

                // rand only produces a long, not a long long, so get jiggy.
                valuer1 = rand();
                valuer2 = rand();
                valuer3 = rand();
                value = (valuer1 * valuer2 * valuer3) % power;
                // overflow will cause some negative values
                if (value < 0) value *= -1;
                if (src_len > 1 && rand() % 2) {
                    // cause ~half of values to be negative, but
                    // reduce length by one to prevent overflow of
                    // src.
                    value /= -10;
                }


#if 0
                BOOST_MESSAGE("src_storage = " << src_storage);
                BOOST_MESSAGE("src_len = " << src_len);
                BOOST_MESSAGE("power = " << power);
                BOOST_MESSAGE("value = " << value);
#endif

                // positive test, "1234   "
                sprintf(buf, "%lld", value);
                BOOST_REQUIRE(strlen(buf) <= src_len);
                testSqlStringCastToExact_Helper(value,
                                                buf,
                                                src_storage,
                                                src_len,
                                                false);

                // positive test, "+123   "
                if (src_len >= 2 && value >= 0) {
                    sprintf(buf, "+%lld", value / 10);
                    BOOST_REQUIRE(strlen(buf) <= src_len);
                    testSqlStringCastToExact_Helper(value / 10,
                                                    buf,
                                                    src_storage,
                                                    src_len,
                                                    false);
                }


                // positive test, "  123", " 1234", "12345", "123456"
                sprintf(buf, "%5lld", value);
                testSqlStringCastToExact_Helper(value,
                                                buf,
                                                src_storage,
                                                src_len,
                                                false);

                // positive test, "            1234"
                sprintf(buf, "%20lld", value);
                testSqlStringCastToExact_Helper(value,
                                                buf,
                                                src_storage,
                                                src_len,
                                                false);

                // positive test, "000000000000001234"
                sprintf(buf, "%020lld", value);
                testSqlStringCastToExact_Helper(value,
                                                buf,
                                                src_storage,
                                                src_len,
                                                false);


                // positive test, "0001234  "
                sprintf(buf, "%07lld", value);
                testSqlStringCastToExact_Helper(value,
                                                buf,
                                                src_storage,
                                                src_len,
                                                false);

                // negative test, "a234   "
                sprintf(buf, "%lld", value);
                buf[0] = 'a';
                testSqlStringCastToExact_Helper(value,
                                                buf,
                                                src_storage,
                                                src_len,
                                                true);

                // negative test, "1a34   "
                if (src_len > 2) {
                    sprintf(buf, "%lld", value);
                    buf[1] = 'a';
                    testSqlStringCastToExact_Helper(value,
                                                    buf,
                                                    src_storage,
                                                    src_len,
                                                    true);
                }

                // negative test, "1 23 "
                if (src_len > 3 && value >= 100) {
                    sprintf(buf, "%lld", value);
                    buf[1] = ' ';
                    testSqlStringCastToExact_Helper(value,
                                                    buf,
                                                    src_storage,
                                                    src_len,
                                                    true);
                }

                // negative test, "    "
                memset(buf, ' ', src_len);
                testSqlStringCastToExact_Helper(value,
                                                buf,
                                                src_storage,
                                                src_len,
                                                true);

                // negative test, "- 3"
                if (src_len > 3) {
                    sprintf(buf, "%lld", value);
                    buf[0] = '-';
                    buf[1] = ' ';
                    testSqlStringCastToExact_Helper(value,
                                                    buf,
                                                    src_storage,
                                                    src_len,
                                                    true);
                }

                // negative test, "+ 3"
                if (src_len > 3) {
                    sprintf(buf, "%lld", value);
                    buf[0] = '-';
                    buf[1] = ' ';
                    testSqlStringCastToExact_Helper(value,
                                                    buf,
                                                    src_storage,
                                                    src_len,
                                                    true);
                }

                // negative test, "- "
                memset(buf, ' ', src_len);
                buf[0] = '-';
                testSqlStringCastToExact_Helper(value,
                                                buf,
                                                src_storage,
                                                src_len,
                                                true);
                // negative test, "+ "
                memset(buf, ' ', src_len);
                buf[0] = '+';
                testSqlStringCastToExact_Helper(value,
                                                buf,
                                                src_storage,
                                                src_len,
                                                true);


            }
        }
    }
}

void
SqlStringTest::testSqlStringCastToDecimal_Helper(uint64_t value,
                                                 int precision,
                                                 int scale,
                                                 char const * const buf,
                                                 int src_storage,
                                                 int src_len,
                                                 bool outOfRangeExpected,
                                                 bool invalidCharExpected)
{
    bool caught = false;
    int64_t newvalue;
    SqlStringBuffer src(src_storage, src_len,
                        0, src_storage-src_len,
                        's', ' ');

#if 0
    BOOST_MESSAGE("buf = |" << buf << "|");
#endif

    if (strlen(buf) > src_len) {
        // not all test cases will fit, just silently ignore them
        return;
    }

    // copy string, minus null
    memcpy(src.mStr, buf, strlen(buf));
    // pad out any leftovers with spaces (say, if value is very small for 
    // string length)
    memset(src.mStr + strlen(buf), ' ', src_len - strlen(buf));
#if 0
    BOOST_MESSAGE("str = |" << src.mLeftP << "|");
#endif
    
    try {
        newvalue = SqlStrCastToExact<1,1>(src.mStr,
                                          src_len,
                                          precision,
                                          scale);
    } catch (const char *str) {
        caught = true;
        if (outOfRangeExpected) {
            BOOST_CHECK_EQUAL(strcmp(str, "22003"), 0);
        } else if (invalidCharExpected) {
            BOOST_CHECK_EQUAL(strcmp(str, "22018"), 0);
        } else {
            // Unexpected exception
            BOOST_CHECK(false);
        }
    }
    BOOST_CHECK_EQUAL(caught, (invalidCharExpected || outOfRangeExpected));
    if (!caught) {
        BOOST_CHECK_EQUAL(value, newvalue);
        BOOST_CHECK(src.verify());
    }
}


// tests varchar case, at least partially, when src_len == src_storage
void
SqlStringTest::testSqlStringCastToDecimal()
{
    int src_storage, src_len;
    int rand_idx;
    int precision, scale;
    int64_t power, poweridx;
    int64_t value, valuer1, valuer2, valuer3;
    char buf[256];
    
    src_storage = MAXLEN;
    //    strlen(2^64) = 20;
    for (src_storage = 1; src_storage <= 20; src_storage++) {
        for (src_len = 1; src_len <= src_storage; src_len++) {
            power = 1;
            for (poweridx = 0; poweridx < src_len; poweridx++) {
                power *= 10;
            }
            // do a bit more than typical random to get decent coverage
            // on positives, negatives, and various length numbers.
            // besides, test runs very quickly anyway.
            for (rand_idx = 0; rand_idx < 5 * MAXRANDOM; rand_idx++) {

                // rand only produces a long, not a long long, so get jiggy.
                valuer1 = rand();
                valuer2 = rand();
                valuer3 = rand();
                value = (valuer1 * valuer2 * valuer3) % power;
                // overflow will cause some negative values
                if (value < 0) value *= -1;
                if (src_len > 1 && rand() % 2) {
                    // cause ~half of values to be negative, but
                    // reduce length by one to prevent overflow of
                    // src.
                    value /= -10;
                }

#if 0
                BOOST_MESSAGE("src_storage = " << src_storage);
                BOOST_MESSAGE("src_len = " << src_len);
                BOOST_MESSAGE("power = " << power);
                BOOST_MESSAGE("value = " << value);
#endif

                scale = 0;

                // positive test, "1234   "
                sprintf(buf, "%lld", value);
                precision = (value < 0)? strlen(buf)-1: strlen(buf);
                BOOST_REQUIRE(strlen(buf) <= src_len);
                testSqlStringCastToDecimal_Helper(value,
                                                  precision,
                                                  scale,
                                                  buf,
                                                  src_storage,
                                                  src_len,
                                                  false,
                                                  false);

                // positive test, "+123   "
                if (src_len >= 2 && value >= 0) {
                    sprintf(buf, "+%lld", value / 10);
                    BOOST_REQUIRE(strlen(buf) <= src_len);
                    testSqlStringCastToDecimal_Helper(value / 10,
                                                      precision,
                                                      scale,
                                                      buf,
                                                      src_storage,
                                                      src_len,
                                                      false,
                                                      false);
                }


                // positive test, "  123", " 1234", "12345", "123456"
                sprintf(buf, "%5lld", value);
                testSqlStringCastToDecimal_Helper(value,
                                                  precision,
                                                  scale,
                                                  buf,
                                                  src_storage,
                                                  src_len,
                                                  false,
                                                  false);

                // positive test, "            1234"
                sprintf(buf, "%20lld", value);
                testSqlStringCastToDecimal_Helper(value,
                                                  precision,
                                                  scale,
                                                  buf,
                                                  src_storage,
                                                  src_len,
                                                  false,
                                                  false);

                // positive test, "000000000000001234"
                sprintf(buf, "%020lld", value);
                testSqlStringCastToDecimal_Helper(value,
                                                  precision,
                                                  scale,
                                                  buf,
                                                  src_storage,
                                                  src_len,
                                                  false,
                                                  false);


                // positive test, "0001234  "
                sprintf(buf, "%07lld", value);
                testSqlStringCastToDecimal_Helper(value,
                                                  precision,
                                                  scale,
                                                  buf,
                                                  src_storage,
                                                  src_len,
                                                  false,
                                                  false);

                // positive test, ".1234"
                sprintf(buf, ".%lld", value);
                if (value < 0) {
                    buf[0] = '-';
                    buf[1] = '.';
                }
                testSqlStringCastToDecimal_Helper(value,
                                                  precision,
                                                  precision,
                                                  buf,
                                                  src_storage+1,
                                                  src_len+1,
                                                  false,
                                                  false);
            


                // positive test, ".1234e3" = "123.4"
                sprintf(buf, ".%llde3", value);
                if (value < 0) {
                    buf[0] = '-';
                    buf[1] = '.';
                }
                testSqlStringCastToDecimal_Helper(value,
                                                  precision,
                                                  precision-3,
                                                  buf,
                                                  src_storage+3,
                                                  src_len+3,
                                                  false,
                                                  false);

                if (value != 0) {
                    // negative test, out of range
                    testSqlStringCastToDecimal_Helper(value,
                                                      precision,
                                                      precision,
                                                      buf,
                                                      src_storage+3,
                                                      src_len+3,
                                                      true,
                                                      false);
                }

                // positive test, "1234e-3"
                uint64_t tmp;
                sprintf(buf, "%llde-3", value);
                
                testSqlStringCastToDecimal_Helper(value,
                                                  precision,
                                                  3,
                                                  buf,
                                                  src_storage,
                                                  src_len,
                                                  false,
                                                  false);
                
                // positive test, rounding
                if (value < 0) {
                    tmp = -((-value + 5)/10);
                } else {
                    tmp = (value + 5)/10;
                }
                testSqlStringCastToDecimal_Helper(tmp,
                                                  precision,
                                                  2,
                                                  buf,
                                                  src_storage+3,
                                                  src_len+3,
                                                  false,
                                                  false);
                
                if (value != 0) {
                    // negative test, out of range
                    testSqlStringCastToDecimal_Helper(value,
                                                      precision,
                                                      4,
                                                      buf,
                                                      src_storage+3,
                                                      src_len+3,
                                                      true,
                                                      false);
                }
            

                // negative test, out of range
                if (abs(value) >= 10) {
                    sprintf(buf, "%lld", value);                    
                    testSqlStringCastToDecimal_Helper(value,
                                                      precision-1,
                                                      scale,
                                                      buf,
                                                      src_storage,
                                                      src_len,
                                                      true,
                                                      false);
                }                

                // negative test, "123e"
                sprintf(buf, "%llde", value);
                testSqlStringCastToDecimal_Helper(value,
                                                  precision,
                                                  scale,
                                                  buf,
                                                  src_storage+1,
                                                  src_len+1,
                                                  false,
                                                  true);
            
                // negative test, "a234   "
                sprintf(buf, "%lld", value);
                buf[0] = 'a';
                testSqlStringCastToDecimal_Helper(value,
                                                  precision,
                                                  scale,
                                                  buf,
                                                  src_storage,
                                                  src_len,
                                                  false,
                                                  true);

                // negative test, "1a34   "
                if (src_len > 2) {
                    sprintf(buf, "%lld", value);
                    buf[1] = 'a';
                    testSqlStringCastToDecimal_Helper(value,
                                                      precision,
                                                      scale,
                                                      buf,
                                                      src_storage,
                                                      src_len,
                                                      false,
                                                      true);
                }

                // negative test, "1 23 "
                if (src_len > 3 && value >= 100) {
                    sprintf(buf, "%lld", value);
                    buf[1] = ' ';
                    testSqlStringCastToDecimal_Helper(value,
                                                      precision,
                                                      scale,
                                                      buf,
                                                      src_storage,
                                                      src_len,
                                                      false,
                                                      true);
                }

                // negative test, "    "
                memset(buf, ' ', src_len);
                testSqlStringCastToDecimal_Helper(value,
                                                  precision,
                                                  scale,
                                                  buf,
                                                  src_storage,
                                                  src_len,
                                                  false,
                                                  true);

                // negative test, "- 3"
                if (src_len > 3) {
                    sprintf(buf, "%lld", value);
                    buf[0] = '-';
                    buf[1] = ' ';
                    testSqlStringCastToDecimal_Helper(value,
                                                      precision,
                                                      scale,
                                                      buf,
                                                      src_storage,
                                                      src_len,
                                                      false,
                                                      true);
                }

                // negative test, "+ 3"
                if (src_len > 3) {
                    sprintf(buf, "%lld", value);
                    buf[0] = '-';
                    buf[1] = ' ';
                    testSqlStringCastToDecimal_Helper(value,
                                                      precision,
                                                      scale,
                                                      buf,
                                                      src_storage,
                                                      src_len,
                                                      false,
                                                      true);
                }

                // negative test, "- "
                memset(buf, ' ', src_len);
                buf[0] = '-';
                testSqlStringCastToDecimal_Helper(value,
                                                  precision,
                                                  scale,
                                                  buf,
                                                  src_storage,
                                                  src_len,
                                                  false,
                                                  true);
                // negative test, "+ "
                memset(buf, ' ', src_len);
                buf[0] = '+';
                testSqlStringCastToDecimal_Helper(value,
                                                  precision,
                                                  scale,
                                                  buf,
                                                  src_storage,
                                                  src_len,
                                                  false,
                                                  true);


            }
        }
    }
}

void
SqlStringTest::testSqlStringCastToApprox_Helper(double value,
                                                char const * const buf,
                                                int src_storage,
                                                int src_len,
                                                bool exceptionExpected)
{
    bool caught = false;
    double newvalue = 0;
    SqlStringBuffer src(src_storage, src_len,
                        0, src_storage-src_len,
                        's', ' ');

#if 0
    BOOST_MESSAGE("buf = |" << buf << "|");
    {
        
        char foo[256];
        sprintf(foo, "%.8lf", value);
        BOOST_MESSAGE("expected value = " << value << " " << foo);
    }
#endif
    if (strlen(buf) > src_len) {
        // not all test cases will fit, just silently ignore them
        return;
    }

    // copy string, minus null
    memcpy(src.mStr, buf, strlen(buf));
    // pad out any leftovers with spaces (say, if value is very small for 
    // string length)
    memset(src.mStr + strlen(buf), ' ', src_len - strlen(buf));
#if 0
    BOOST_MESSAGE("str = |" << src.mLeftP << "|");
#endif
    
    try {
        newvalue = SqlStrCastToApprox<1,1>(src.mStr,
                                           src_len);
    } catch (const char *str) {
        caught = true;
        BOOST_CHECK_EQUAL(strcmp(str, "22018"), 0);
    }
    BOOST_CHECK_EQUAL(caught, exceptionExpected);
    if (!caught) {
        // absolute epsilon doesn't make sense, must be relative
        // to size of numbers being compared. (trying to emulate
        // some sort of absolute mantissa, but have exponent adjust
        // accordingly.)
        double epsilon = fabs(value / 10000);
        if (epsilon < 0.0001) {
            // set a floor to epsilon so it doesn't get rounded out as well.
            epsilon = 0.0001;
        }
        BOOST_CHECK(fabs(value - newvalue) < epsilon);
        BOOST_CHECK(src.verify());
    }
}

// tests varchar case, at least partially, when src_len == src_storage
void
SqlStringTest::testSqlStringCastToApprox()
{
    int src_storage, src_len;
    char exponent_buf[256];
    char decimal_buf[256];
    char neg_buf[256];
    double orig_value, dec_value, exp_value;
    double small_idx;
    int leading;
    int leadingplus;
    int leadingminus;
    int beforedec;
    int afterdec;
    int src_len_left;
    int buildlen;
    int idx;
    int rnd;
    int neg_idx;
    
    src_storage = MAXLEN;
    // No point in strings less than 3 bytes long (0.0, +3., -.4 or 1E0 is
    // kinda the minimal practial representation)
    for (src_storage = 2; src_storage <= 10; src_storage++) {
        for (src_len = 2; src_len <= src_storage; src_len++) {
            for (leading = 0; leading <= 2; leading++) {
                leadingplus = leadingminus = 0;
                if (leading == 1 && src_len > 2) {
                    leadingplus = 1;
                }
                if (leading == 2 && src_len > 2) {
                    leadingminus = 1;
                }
                src_len_left = src_len;
                if (leadingplus || leadingminus) src_len_left--;
                for (beforedec = 0; beforedec <= src_len_left; beforedec++) {
                    afterdec = src_len_left - beforedec;

#if 0
                    BOOST_MESSAGE("src_storage = " << src_storage);
                    BOOST_MESSAGE("src_len = " << src_len);
                    BOOST_MESSAGE("leadingplus = " << leadingplus);
                    BOOST_MESSAGE("leadingminus = " << leadingminus);
                    BOOST_MESSAGE("beforedec = " << beforedec);
                    BOOST_MESSAGE("afterdec = " << afterdec);
#endif
                    
                    buildlen = leadingplus + leadingminus + beforedec + 
                        afterdec;
                    BOOST_REQUIRE(buildlen == src_len);

                    string s;
                    if (leadingplus) s.append("+");
                    if (leadingminus) s.append("-");
                    idx = beforedec;
                    while (idx-- > 0) {
                        rnd = rand() % 10;
                        s.append(1, '0' + rnd);
                    }
                    if (afterdec) {
                        s.append(".");
                        idx = afterdec - 1;
                        while (idx-- > 0) {
                            rnd = rand() % 10;
                            s.append(1, '0' + rnd);
                        }
                    }
                    sscanf(s.c_str(), "%lf", &orig_value);

                    for (small_idx = 1E+10;
                         small_idx > 1E-10;
                         small_idx *= 0.01) {
                        dec_value = orig_value * small_idx;
                        // TODO: This masks the + in string s above.
                        sprintf(decimal_buf, "%.8lf", orig_value * small_idx);
                        sscanf(decimal_buf, "%lf", &dec_value);
                        sprintf(exponent_buf, "%.8E", orig_value * small_idx);
                        sscanf(exponent_buf, "%lf", &exp_value);
                    
#if 0
                        BOOST_MESSAGE("s = |" << s << "|");
                        BOOST_MESSAGE("exponent_buf = |"<< exponent_buf<< "|");
                        BOOST_MESSAGE("dec_value = " << dec_value);
                        BOOST_MESSAGE("exp_value = " << exp_value);
#endif


                        // positive test, "12E34   "
                        testSqlStringCastToApprox_Helper(exp_value,
                                                         exponent_buf,
                                                         src_storage,
                                                         src_len,
                                                         false);
                        // positive test, "12.34   "
                        testSqlStringCastToApprox_Helper(dec_value,
                                                         decimal_buf,
                                                         src_storage,
                                                         src_len,
                                                         false);

                        // positive test, "   12E34   "
                        sprintf(exponent_buf, "%10.8E",
                                orig_value * small_idx);
                        sscanf(exponent_buf, "%lf", &exp_value);
                        testSqlStringCastToApprox_Helper(exp_value,
                                                         exponent_buf,
                                                         src_storage,
                                                         src_len,
                                                         false);

                        // positive test, "   12.34   "
                        sprintf(decimal_buf, "%10.8lf",
                                orig_value * small_idx);
                        sscanf(decimal_buf, "%lf", &dec_value);
                        testSqlStringCastToApprox_Helper(dec_value,
                                                         decimal_buf,
                                                         src_storage,
                                                         src_len,
                                                         false);

                        // positive test, "00012E34   "
                        sprintf(exponent_buf, "%010.8E",
                                orig_value * small_idx);
                        sscanf(exponent_buf, "%lf", &exp_value);
                        testSqlStringCastToApprox_Helper(exp_value,
                                                         exponent_buf,
                                                         src_storage,
                                                         src_len,
                                                         false);

                        // positive test, "00012.34   "
                        sprintf(decimal_buf, "%010.8lf",
                                orig_value * small_idx);
                        sscanf(decimal_buf, "%lf", &dec_value);
                        testSqlStringCastToApprox_Helper(dec_value,
                                                         decimal_buf,
                                                         src_storage,
                                                         src_len,
                                                         false);

                        // don't do negative tests every time as they
                        // are highly highly redundant. good coverage
                        // at small sizes, then taper way off.
                        if (src_storage < 4 || !(rand() % 10)) {
                            // get back to base values
                            sprintf(decimal_buf, "%.8lf",
                                    orig_value * small_idx);
                            sscanf(decimal_buf, "%lf",
                                   &dec_value);
                            sprintf(exponent_buf, "%.8E",
                                    orig_value * small_idx);
                            sscanf(exponent_buf, "%lf", &exp_value);
                            int exp_len = strlen(exponent_buf);
                            int dec_len = strlen(decimal_buf);

                            for (neg_idx = 0;
                                 neg_idx < dec_len; 
                                 neg_idx++) {

                                strcpy(neg_buf, decimal_buf);
                                neg_buf[neg_idx] = 'a';
                                testSqlStringCastToApprox_Helper(dec_value,
                                                                 neg_buf,
                                                                 src_storage,
                                                                 src_len,
                                                                 true);
                                if (neg_idx > 1 && neg_idx < dec_len - 1) {
                                    // leading and trailing spaces are OK
                                    neg_buf[neg_idx] = ' ';
                                    testSqlStringCastToApprox_Helper(dec_value,
                                                                     neg_buf,
                                                                     src_storage,
                                                                     src_len,
                                                                     true);
                                }
                            }

                            for (neg_idx = 0;
                                 neg_idx < exp_len;
                                 neg_idx++) {

                                strcpy(neg_buf, exponent_buf);
                                neg_buf[neg_idx] = 'a';
                                testSqlStringCastToApprox_Helper(exp_value,
                                                                 neg_buf,
                                                                 src_storage,
                                                                 src_len,
                                                                 true);
                                if (neg_idx > 1 && neg_idx < exp_len - 1) {
                                    // leading and trailing spaces are OK
                                    neg_buf[neg_idx] = ' ';
                                    testSqlStringCastToApprox_Helper(exp_value,
                                                                     neg_buf,
                                                                     src_storage,
                                                                     src_len,
                                                                     true);
                                }
                            }

                            // negative test, "    "
                            memset(neg_buf, ' ', src_len);
                            testSqlStringCastToApprox_Helper(exp_value,
                                                             neg_buf,
                                                             src_storage,
                                                             src_len,
                                                             true);
                        }
                    }
                }
            }
        }
    }
}

void
SqlStringTest::testSqlStringCastFromExact()
{
    int src_len;
    int dst_storage, dst_len, newlen = 0;
    int rand_idx, power_idx;
    int negative;
    int64_t value, newones;
    char expected_buf[256];
    bool caught;
    
    // strlen(MAX_VAL(int64_t))=19, strlen(MIN_VAL(int64_t))=20
    for (dst_storage = 0; dst_storage <= 22; dst_storage++) {
        for (dst_len = 0; dst_len <= dst_storage; dst_len++) {
            for (src_len = 0; src_len < 19; src_len++) {
                for (rand_idx = 0; rand_idx < 1; rand_idx++) {
                    for (negative = 0; negative <= 1; negative++) {

                        value = 0;
                        for (power_idx = 0; 
                             power_idx < src_len - negative; // space for '-'
                             power_idx++) {
                            if (!value) {
                                // no leading zeros
                                newones = rand() % 9 + 1;
                            } else {
                                newones = rand() % 10;
                            }
                            value = value*10 + newones;
                        }
                        if (!(rand() % 10)) value = 0; // goose odds of 0
                        if (negative) { 
                            value *= -1;
                        }
                        
                        sprintf(expected_buf, "%lld", value);
                        string expect(expected_buf);
                        string expect_fix(expect); // right padded (CHAR)
                        if (expect_fix.length() < dst_storage) {
                            expect_fix.append(dst_storage -
                                              expect_fix.length(),
                                              ' ');
                        }

                        SqlStringBuffer dst(dst_storage, dst_len,
                                            0, dst_storage - dst_len,
                                            's', ' ');

                        caught = false;
                        try {
                            newlen = SqlStrCastFromExact<1,1>(dst.mStr,
                                                              dst_storage,
                                                              value,
                                                              false);
                        } catch (const char *str) {
                            caught = true;
                            BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                            BOOST_CHECK(expect.length() > dst_storage);
                            BOOST_CHECK(dst.verify());
                        }
                        if (!caught) {
                            string result(dst.mStr, newlen);
                            BOOST_CHECK(dst.verify());
                            BOOST_CHECK(expect.length() <= dst_storage);
                            BOOST_CHECK(!expect.compare(result));
                        }

                        SqlStringBuffer dst_fix(dst_storage, dst_len,
                                                0, dst_storage - dst_len,
                                                's', ' ');

                        caught = false;
                        try {
                            newlen = SqlStrCastFromExact<1,1>(dst_fix.mStr,
                                                              dst_storage,
                                                              value,
                                                              true);
                        } catch (const char *str) {
                            caught = true;
                            BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                            BOOST_CHECK(expect_fix.length() > dst_storage);
                            BOOST_CHECK(dst_fix.verify());
                        }
                        if (!caught) {
                            string result_fix(dst_fix.mStr, newlen);
                            BOOST_CHECK(dst_fix.verify());
                            BOOST_CHECK(expect_fix.length() <= dst_storage);
                            BOOST_CHECK(!expect_fix.compare(result_fix));
                        }
                    }
                }
            }
        }
    }
}

void
SqlStringTest::testSqlStringCastFromDecimal()
{
    int precision, scale;
    int src_len;
    int dst_storage, dst_len, newlen = 0;
    int rand_idx, power_idx;
    int negative;
    int64_t value, newones, whole, decimal;
    char expected_buf[256];
    char digits[] = "0123456789";
    bool caught;
    
    // strlen(MAX_VAL(int64_t))=19, strlen(MIN_VAL(int64_t))=20
    for (dst_storage = 0; dst_storage <= 22; dst_storage++) {
        for (dst_len = 0; dst_len <= dst_storage; dst_len++) {
            for (src_len = 0; src_len < 19; src_len++) {
                for (rand_idx = 0; rand_idx < 1; rand_idx++) {
                    for (negative = 0; negative <= 1; negative++) {
                        precision = src_len;
                        value = 0;
                        for (power_idx = 0; 
                             power_idx < src_len - negative; // space for '-'
                             power_idx++) {
                            if (!value) {
                                // no leading zeros
                                newones = rand() % 9 + 1;
                            } else {
                                newones = rand() % 10;
                            }
                            value = value*10 + newones;
                        }
                        if (!(rand() % 10)) value = 0; // goose odds of 0
                        if (negative) { 
                            value *= -1;
                        }
                        scale = rand() % 25 - 5;

                        if (scale == 0) {
                            sprintf(expected_buf, "%lld", value);
                        } else if (scale > 0) {
                            whole = value;
                            for (int i = 0; i < scale; i++) {
                                whole /= 10;
                            }

                            if (whole != 0) {
                                sprintf(expected_buf, "%lld", whole);
                            } else {
                                if (value < 0) {
                                    expected_buf[0] = '-';
                                    expected_buf[1] = '\0';
                                } else {
                                    expected_buf[0] = '\0';
                                }
                            }

                            for (int i = 0; i < scale; i++) {
                                whole *= 10;
                            }
                            decimal = abs(value - whole);

                            int len = strlen(expected_buf);
                            expected_buf[len] = '.';
                            for (int i = scale-1; i >= 0; i--) {
                                expected_buf[len+i+1] = digits[decimal % 10];
                                decimal /= 10;
                            }
                            expected_buf[len+scale+1] = '\0';
                        } else if (scale < 0) {
                            sprintf(expected_buf, "%lld", value);
                            if (value != 0) {
                                int len = strlen(expected_buf);
                                memset(expected_buf + len, '0', -scale);
                                expected_buf[len - scale] = '\0';
                            }
                        }

                        string expect(expected_buf);
                        string expect_fix(expect); // right padded (CHAR)
                        if (expect_fix.length() < dst_storage) {
                            expect_fix.append(dst_storage -
                                              expect_fix.length(),
                                              ' ');
                        }

                        SqlStringBuffer dst(dst_storage, dst_len,
                                            0, dst_storage - dst_len,
                                            's', ' ');

                        caught = false;
                        try {
                            newlen = SqlStrCastFromExact<1,1>(dst.mStr,
                                                              dst_storage,
                                                              value,
                                                              precision,
                                                              scale,
                                                              false);
                        } catch (const char *str) {
                            caught = true;
                            BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                            BOOST_CHECK(expect.length() > dst_storage);
                            BOOST_CHECK(dst.verify());
                        }
                        if (!caught) {
                            string result(dst.mStr, newlen);
                            BOOST_CHECK(dst.verify());
                            BOOST_CHECK(expect.length() <= dst_storage);
                            BOOST_CHECK(!expect.compare(result));
                        }

                        SqlStringBuffer dst_fix(dst_storage, dst_len,
                                                0, dst_storage - dst_len,
                                                's', ' ');

                        caught = false;
                        try {
                            newlen = SqlStrCastFromExact<1,1>(dst_fix.mStr,
                                                              dst_storage,
                                                              value,
                                                              precision,
                                                              scale,
                                                              true);
                        } catch (const char *str) {
                            caught = true;
                            BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                            BOOST_CHECK(expect_fix.length() > dst_storage);
                            BOOST_CHECK(dst_fix.verify());
                        }
                        if (!caught) {
                            string result_fix(dst_fix.mStr, newlen);
                            BOOST_CHECK(dst_fix.verify());
                            BOOST_CHECK(expect_fix.length() <= dst_storage);
                            BOOST_CHECK(!expect_fix.compare(result_fix));
                        }
                    }
                }
            }
        }
    }
}

void
SqlStringTest::testSqlStringCastFromApprox()
{
    int src_len;
    int dst_storage, dst_len, newlen = 0;
    int rand_idx, power_idx;
    int negative;
    int isFloat;
    int max_precision;
    double value, newones, exponent;
    char expected_buf[256];
    bool caught;
    
    // strlen(%E.16) = 22
    for (dst_storage = 0; dst_storage <= 24; dst_storage++) {
        for (dst_len = 0; dst_len <= dst_storage; dst_len++) {
            // double seems to have ~16 digits of precision
            for (src_len = 0; src_len < 16; src_len++) {
                for (rand_idx = 0; rand_idx < 1; rand_idx++) {
                    for (negative = 0; negative <= 1; negative++) {
                        value = 0;
                        for (power_idx = 0; 
                             // space for (optional) '-' '.', 'E+xx'
                             power_idx < src_len - (negative + 5);
                             power_idx++) {
                            if (!value) {
                                // no leading zeros
                                newones = rand() % 9 + 1;
                            } else {
                                newones = rand() % 10;
                            }
                            value = value*10 + newones;
                        }
                        if (!(rand() % 10)) value = 0; // goose odds of 0
                        if (negative) { 
                            value *= -1;
                        }
                        // get some exponent on.
                        exponent = 1 + rand() % 30;
                        if (!(rand() % 2)) {
                            exponent *= -1;
                        }
                        value = pow(value, 1 + rand() % 80);

                        isFloat = rand() % 2;
                        if (isFloat) {
                            value = (float) value;
                        }
                        max_precision = (isFloat)? 7: 16;

                        if (value) {
                            int i, epos = -1, prec = 0, buflen, exp;
                            int neg = (value < 0)? 1: 0;
                            char last_digit = 0;
                            sprintf(expected_buf, "%.*E", max_precision, value);
                            buflen = strlen(expected_buf);
                            epos = neg + max_precision + 2;
                            if (buflen > epos && expected_buf[epos] == 'E') {
                                sscanf(expected_buf + epos + 1, "%d", &exp);

                                // Round up if needed
                                if ((expected_buf[epos-1] >= '5') && 
                                    (expected_buf[epos-1] <= '9')) {
                                    expected_buf[epos-1] = '0';
                                    for (int i=epos-2; i>=neg; i--) {
                                        if (expected_buf[i] == '9') {
                                            expected_buf[i] = '0';
                                        } else if (expected_buf[i] != '.') {
                                            expected_buf[i]++;
                                            break;
                                        }
                                    }
                                    
                                    // See if initial digit overflowed
                                    if (expected_buf[neg] == '0') {
                                        expected_buf[neg] = '1';
                                        for (int i=epos-1; i>neg+2; i--) {
                                            expected_buf[i] = expected_buf[i-1];
                                        }
                                        expected_buf[neg+2] = '0';
                                        
                                        // increment exponent
                                        exp++;
                                    }
                                }
                                
                                for (i = epos-2; i >= 0; i--) {
                                    if (expected_buf[i] != '0') {
                                        if (expected_buf[i] == '.') {
                                            BOOST_CHECK_EQUAL(i, 1+neg);
                                            last_digit = expected_buf[i-1];
                                        } else {
                                            last_digit = expected_buf[i];
                                        }
                                        prec = i-1-neg;
                                        break;
                                    }
                                }
                                sprintf(expected_buf, "%.*E", prec, value);
                                buflen = strlen(expected_buf);
                                epos = (prec > 0)? (neg + prec + 2):(neg + 1);
                                BOOST_CHECK(buflen > epos);
                                BOOST_CHECK(expected_buf[epos] == 'E');
                                expected_buf[epos-1] = last_digit;
                                sprintf(expected_buf + epos + 1, "%d", exp);
                            }
                        } else {
                            // per spec, 0 -> '0E0'
                            strcpy(expected_buf, "0E0");
                        }

                        string expect(expected_buf);
                        string expect_fix(expect); // right padded (CHAR)
                        if (expect_fix.length() < dst_storage) {
                            expect_fix.append(dst_storage -
                                              expect_fix.length(),
                                              ' ');
                        }

#if 0
                        BOOST_MESSAGE("value = " << value);
                        BOOST_MESSAGE("storage = " << dst_storage << 
                                      " len = " << dst_len <<
                                      " src_len = " << src_len);
                        BOOST_MESSAGE("expect = |" << expect << "|");
                        BOOST_MESSAGE("expect_fix = |" << expect_fix << "|");
#endif

                        SqlStringBuffer dst(dst_storage, dst_len,
                                            0, dst_storage - dst_len,
                                            's', ' ');

                        caught = false;
                        try {
                            newlen = SqlStrCastFromApprox<1,1>(dst.mStr,
                                                               dst_storage,
                                                               value,
                                                               isFloat,
                                                               false);
                        } catch (const char *str) {
                            caught = true;
                            BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                            BOOST_CHECK(expect.length() > dst_storage);
                            BOOST_CHECK(dst.verify());
                        }
                        if (!caught) {
                            string result(dst.mStr, newlen);
                            //BOOST_MESSAGE("result = |" << result << "|");
                            BOOST_CHECK(dst.verify());
                            BOOST_CHECK(expect.length() <= dst_storage);
                            BOOST_CHECK(!expect.compare(result));
                            if (expect.compare(result)) {
                                BOOST_MESSAGE("Got " << result << 
                                              ", expected " << expect);
                            }
                        }

                        SqlStringBuffer dst_fix(dst_storage, dst_len,
                                                0, dst_storage - dst_len,
                                                's', ' ');

                        caught = false;
                        try {
                            newlen = SqlStrCastFromApprox<1,1>(dst_fix.mStr,
                                                               dst_storage,
                                                               value,
                                                               isFloat,
                                                               true);
                        } catch (const char *str) {
                            caught = true;
                            BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                            BOOST_CHECK(expect_fix.length() > dst_storage);
                            BOOST_CHECK(dst_fix.verify());
                        }
                        if (!caught) {
                            string result_fix(dst_fix.mStr, newlen);
                            //BOOST_MESSAGE("result_fix = |" << result_fix << "|");
                            BOOST_CHECK(dst_fix.verify());
                            BOOST_CHECK(expect_fix.length() <= dst_storage);
                            BOOST_CHECK(!expect_fix.compare(result_fix));
                        }
                    }
                }
            }
        }
    }
}


void
SqlStringTest::testSqlStringCastToVarChar()
{
    int src_storage, dst_storage, src_len, dst_len;

    for (dst_storage = 0; dst_storage < MAXLEN; dst_storage++) {
        for (dst_len = 0; dst_len <= dst_storage; dst_len++) {
            for (src_storage = 0; src_storage < MAXLEN; src_storage++) {
                for (src_len = 0; src_len <= src_storage; src_len++) {
                    // ASCII

                    SqlStringBuffer dst(dst_storage, dst_len,
                                        0, dst_storage - dst_len,
                                        'd', ' ');

                    SqlStringBuffer src(src_storage, src_len,
                                        0, src_storage - src_len,
                                        's', ' ');

                    int rightTruncWarning = 0;
                    try {
                        SqlStrCastToVarChar<1,1>(dst.mStr,
                                                 dst_storage,
                                                 src.mStr,
                                                 src_len,
                                                 &rightTruncWarning);
                    } catch (...) {
                        BOOST_CHECK(false);
                    }

                    BOOST_CHECK(
                                (src_len <= dst_storage && !rightTruncWarning) ||
                                (src_len > dst_storage && rightTruncWarning));

                    string expect;
                    expect.append(min(src_len, dst_storage), 's');

                    if (dst_storage > src_len) {
                        expect.append(dst_storage - src_len, ' ');
                    }

                    string result(dst.mStr, dst_storage);

#if 0
                    BOOST_MESSAGE(" dst_storage=" << dst_storage <<
                                  " dst_len=" << dst_len <<
                                  " src_storage=" << src_storage <<
                                  " src_len=" << src_len);
                    BOOST_MESSAGE("src =|" << src.mLeftP << "|");
                    BOOST_MESSAGE("expect |" << expect << "|");
                    BOOST_MESSAGE("result |" << result << "|");
#endif
                    
                    BOOST_CHECK(!result.compare(expect));

                    BOOST_CHECK(dst.verify());
                    BOOST_CHECK(src.verify());

                    // REVIEW: SZ: 8/10/2004: add testing UCS2 support
                    // once it exists.
                }
            }
        }
    }
}


void
SqlStringTest::testSqlStringCastToChar()
{
    int src_storage, dst_storage, src_len, dst_len, new_len = 0;

    // REVIEW: SZ: 8/10/2004: I believe that we should also test
    // unpadded varchars -- e.g. where src_storage > src_len and
    // left_padding == right_padding = 0.  The cast behaves
    // differently in this case, but SqlStringBuffer asserts when
    // length + padding != storage

    for (dst_storage = 0; dst_storage < MAXLEN; dst_storage++) {
        for (dst_len = 0; dst_len <= dst_storage; dst_len++) {
            for (src_storage = 0; src_storage < MAXLEN; src_storage++) {
                for (src_len = 0; src_len <= src_storage; src_len++) {
                    // ASCII
                    
                    SqlStringBuffer dst(dst_storage, dst_len,
                                        0, dst_storage - dst_len,
                                        'd', ' ');
                    
                    SqlStringBuffer src(src_storage, src_len,
                                        0, src_storage - src_len,
                                        's', ' ');

                    int rightTruncWarning = 0;

                    try {
                        new_len = SqlStrCastToChar<1,1>(dst.mStr,
                                                        dst_storage,
                                                        src.mStr,
                                                        src_len,
                                                        &rightTruncWarning);
                    } catch (...) {
                        BOOST_CHECK(false);
                    }

                    BOOST_CHECK(
                                (src_len <= dst_storage && !rightTruncWarning) ||
                                (src_len > dst_storage && rightTruncWarning));
                    BOOST_CHECK_EQUAL(new_len, dst_storage);
                        
                    string expect;
                    expect.append(min(src_len, dst_storage), 's');

                    if (dst_storage > src_len) {
                        expect.append(dst_storage - src_len, ' ');
                    }

                    string result(dst.mStr, dst_storage);

#if 0
                    BOOST_MESSAGE(" dst_storage=" << dst_storage <<
                                  " dst_len=" << dst_len <<
                                  " src_storage=" << src_storage <<
                                  " src_len=" << src_len);
                    BOOST_MESSAGE("src =|" << src.mLeftP << "|");
                    BOOST_MESSAGE("expect |" << expect << "|");
                    BOOST_MESSAGE("result |" << result << "|");
#endif
                        
                    BOOST_CHECK(!result.compare(expect));

                    BOOST_CHECK(dst.verify());
                    BOOST_CHECK(src.verify());
                
                    // REVIEW: SZ: 8/10/2004: add testing UCS2 support
                    // once it exists.
                }
            }
        }
    }
}


FENNEL_UNIT_TEST_SUITE(SqlStringTest);
