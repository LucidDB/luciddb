/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Technologies, Inc.
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
#include "fennel/calc/SqlStringAscii.h"
#include "fennel/common/TraceSource.h"
#include "fennel/test/SqlStringBuffer.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>

using namespace fennel;
using namespace std;


const int MAXLEN = 8;   // Must not be less than 5. Best >=7.
const int MAXRANDOM = 5;
const int MAXCMPLEN = 8;  // Must not be less than 3.


class SqlStringAsciiTest : virtual public TestBase, public TraceSource
{
    void testSqlStringBuffer_Ascii();

    void testSqlStringCat_Ascii_Fix();
    void testSqlStringCat_Ascii_Var();
    void testSqlStringCat_Ascii_Var2();
    void testSqlStringCmp_Ascii_Fix_DiffLen();
    void testSqlStringCmp_Ascii_Fix_EqLen();
    void testSqlStringCmp_Ascii_Var_DiffLen();
    void testSqlStringCmp_Ascii_Var_EqLen();
    void testSqlStringLenBit_Ascii();
    void testSqlStringLenChar_Ascii();
    void testSqlStringLenOct_Ascii();
    void testSqlStringOverlay_Ascii();
    void testSqlStringPos_Ascii();
    void testSqlStringSubStr_Ascii();
    void testSqlStringToLower_Ascii();
    void testSqlStringToUpper_Ascii();
    void testSqlStringTrim_Ascii();

    void testSqlStringCmp_Ascii_Var_Helper(SqlStringBuffer &src1,
                                           int src1_len,
                                           SqlStringBuffer &src2,
                                           int src2_len);
    void testSqlStringCmp_Ascii_Fix_Helper(SqlStringBuffer &src1,
                                           int src1_storage,
                                           int src1_len,
                                           SqlStringBuffer &src2,
                                           int src2_storage,
                                           int src2_len);
    int testSqlStringNormalizeLexicalCmp(int v);
    
public:
    explicit SqlStringAsciiTest()
        : TraceSource(this,"SqlStringAsciiTest")
    {
        srand(time(NULL));
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringBuffer_Ascii);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringCat_Ascii_Fix);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringCat_Ascii_Var2);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringCat_Ascii_Var);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringCmp_Ascii_Fix_DiffLen);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringCmp_Ascii_Fix_EqLen);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringCmp_Ascii_Var_DiffLen);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringCmp_Ascii_Var_EqLen);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringLenBit_Ascii);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringLenChar_Ascii);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringLenOct_Ascii);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringOverlay_Ascii);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringPos_Ascii);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringSubStr_Ascii);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringToLower_Ascii);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringToUpper_Ascii);
        FENNEL_UNIT_TEST_CASE(SqlStringAsciiTest, testSqlStringTrim_Ascii);
    }
    
    virtual ~SqlStringAsciiTest()
    {
    }
};

void
SqlStringAsciiTest::testSqlStringBuffer_Ascii()
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
                for(k = 0; k < leftbump; k++) {
                    BOOST_CHECK_EQUAL(*(p++), SqlStringBuffer::mBumperChar);
                }
                BOOST_CHECK(p == t.mStr);
                // left padding
                for(k = 0; k < leftpad; k++) {
                    BOOST_CHECK_EQUAL(*(p++), ' ');
                }
                // text
                for (k = 0; k < size; k++) {
                    BOOST_CHECK_EQUAL(*(p++), 'x');
                }
                // right padding
                for(k = 0; k < rightpad; k++) {
                    BOOST_CHECK_EQUAL(*(p++), ' ');
                }
                BOOST_CHECK(p == t.mRightP);
                // right bumper
                for(k = 0; k < rightbump; k++) {
                    BOOST_CHECK_EQUAL(*(p++), SqlStringBuffer::mBumperChar);
                }
                BOOST_CHECK_EQUAL(static_cast<int>(p - t.mLeftP), storage+leftbump+rightbump);
        
                BOOST_CHECK(t.verify());

                for(k = 0; k < size; k++) {
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


// Test catting 3 fixed width strings together as proof-of-concept
void
SqlStringAsciiTest::testSqlStringCat_Ascii_Fix()
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
                                    newlen = SqlStrCat_Ascii(dst.mStr, dst_storage,
                                                             src1.mStr, src1_storage,
                                                             src2.mStr, src2_storage);
                                } catch(const char *str) {
                                    caught = true;
                                    BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                                    BOOST_CHECK(src1_storage + src2_storage > dst_storage);
                                    BOOST_CHECK(dst.verify());
                                    BOOST_CHECK(src1.verify());
                                    BOOST_CHECK(src2.verify());
                                } catch(...) {
                                    BOOST_CHECK(false);
                                }
                                if (!caught) {
                                    BOOST_CHECK(src1_storage + src2_storage <= dst_storage);
                                    BOOST_CHECK(dst.verify());
                                    BOOST_CHECK(src1.verify());
                                    BOOST_CHECK(src2.verify());
                                    
                                    caught = false;
                                    try {
                                        newlen = SqlStrCat_Ascii(dst.mStr,
                                                                 dst_storage,
                                                                 newlen,
                                                                 src3.mStr,
                                                                 src3_storage);
                                    } catch(const char *str) {
                                        caught = true;
                                        BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                                        BOOST_CHECK((src1_storage + 
                                                     src2_storage +
                                                     src3_storage) > dst_storage);
                                        BOOST_CHECK(dst.verify());
                                        BOOST_CHECK(src1.verify());
                                        BOOST_CHECK(src2.verify());
                                        BOOST_CHECK(src3.verify());
                                    } catch(...) {
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
SqlStringAsciiTest::testSqlStringCat_Ascii_Var2()
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
                            newlen = SqlStrCat_Ascii(dst.mStr,
                                                     dst_storage,
                                                     src1.mStr,
                                                     src1_len,
                                                     src2.mStr,
                                                     src2_len);
                        } catch(const char *str) {
                            caught = true;
                            BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                            BOOST_CHECK(src1_len + src2_len > dst_storage);
                        } catch(...) {
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
SqlStringAsciiTest::testSqlStringCat_Ascii_Var()
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
                        newlen = SqlStrCat_Ascii(dst.mStr, 
                                                 dst_storage,
                                                 dst_len,
                                                 src.mStr,
                                                 src_len);
                    } catch(const char *str) {
                        caught = true;
                        BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                        BOOST_CHECK(src_len + dst_len > dst_storage);
                    } catch(...) {
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

int
SqlStringAsciiTest::testSqlStringNormalizeLexicalCmp(int v)
{
    if (v < 0) return -1;
    if (v > 0) return 1;
    return 0;
}



void
SqlStringAsciiTest::testSqlStringCmp_Ascii_Fix_Helper(SqlStringBuffer &src1,
                                            int src1_storage,
                                            int src1_len,
                                            SqlStringBuffer &src2,
                                            int src2_storage,
                                            int src2_len)
{
    int result;
    
    string s1(src1.mStr, src1_len);
    string s2(src2.mStr, src2_len);

    // It is possible that test string ends with a space. Remove it.
    s1.erase (s1.find_last_not_of ( " " ) + 1);
    s2.erase (s2.find_last_not_of ( " " ) + 1);
        
    int expected = testSqlStringNormalizeLexicalCmp(s1.compare(s2));
    char const * const s1p = s1.c_str();
    char const * const s2p = s2.c_str();
    int expected2 = testSqlStringNormalizeLexicalCmp(strcmp(s1p, s2p));
    BOOST_CHECK_EQUAL(expected, expected2);

    result = SqlStrCmp_Ascii_Fix(src1.mStr, src1_storage,
                                 src2.mStr, src2_storage);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());

#if 0
    BOOST_MESSAGE(" src1=|" << s1 << "|" << 
                  " src2=|" << s2 << "|" <<
                  " expect=" << expected <<
                  " expect2=" << expected2 <<
                  " result=" << result);
#endif     
    BOOST_CHECK_EQUAL(result, expected);

    // check the exact opposite, even if equal
    int result2 = SqlStrCmp_Ascii_Fix(src2.mStr, src2_storage,
                                  src1.mStr, src1_storage);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(result2 * -1, result);
    
    // force check of equal strings
    result = SqlStrCmp_Ascii_Fix(src1.mStr, src1_storage,
                             src1.mStr, src1_storage);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK_EQUAL(result, 0);
    
}


void
SqlStringAsciiTest::testSqlStringCmp_Ascii_Fix_DiffLen()
{
    int src1_storage, src2_storage, src1_len, src2_len;
    unsigned char startchar;

    for (src1_storage = 0; src1_storage <= MAXLEN; src1_storage++) {
        for (src1_len = 0; src1_len < src1_storage; src1_len++) {
            for (src2_storage = 0; src2_storage <= MAXLEN; src2_storage++) {
                for (src2_len = 0; src2_len < src2_storage; src2_len++) {
                    // can't test w/ 0, confuses strcmp and/or std:string
                    for (startchar = 1; startchar < 255; startchar++) {
                        SqlStringBuffer src1(src1_storage, src1_len,
                                             0, src1_storage - src1_len, 
                                             'd', ' ');
                        SqlStringBuffer src2(src2_storage, src2_len,
                                             0, src2_storage-src2_len,
                                             's', ' ');
                        
                        src1.patternfill(startchar, 1, 255);
                        src2.patternfill(startchar, 1, 255);
                        
                        testSqlStringCmp_Ascii_Fix_Helper(src1, src1_storage, src1_len,
                                                src2, src2_storage, src2_len);
                    }
                }
            }
        }
    }
}


void
SqlStringAsciiTest::testSqlStringCmp_Ascii_Fix_EqLen()
{
    int src1_storage, src2_storage, src1_len, src2_len, randX;
    
    // not much point large length, chances of 2 random strings being equal
    // are very low. test forces an equality check anyway.
    src1_storage = MAXCMPLEN;
    src2_storage = MAXCMPLEN;
    for (src1_len = 0; src1_len < src1_storage; src1_len++) {
        src2_len = src1_len;
        for (randX = 0; randX <= 65536; randX++) {
            SqlStringBuffer src1(src1_storage, src1_len,
                                 0, src1_storage - src1_len, 
                                 'd', ' ');
            SqlStringBuffer src2(src2_storage, src2_len,
                                 0, src2_storage-src2_len,
                                 's', ' ');
            
            // can't test w/ 0, confuses strcmp and/or std:string
            src1.randomize(1, 1, 255);
            src2.randomize(1, 1, 255);
            
            testSqlStringCmp_Ascii_Fix_Helper(src1, src1_storage, src1_len,
                                    src2, src2_storage, src2_len);
        }
    }
}


void
SqlStringAsciiTest::testSqlStringCmp_Ascii_Var_Helper(SqlStringBuffer &src1,
                                            int src1_len,
                                            SqlStringBuffer &src2,
                                            int src2_len)
{
    int result;
    
    string s1(src1.mStr, src1_len);
    string s2(src2.mStr, src2_len);
        
    int expected = testSqlStringNormalizeLexicalCmp(s1.compare(s2));
    char const * const s1p = s1.c_str();
    char const * const s2p = s2.c_str();
    int expected2 = testSqlStringNormalizeLexicalCmp(strcmp(s1p, s2p));
    BOOST_CHECK_EQUAL(expected, expected2);
    

    result = SqlStrCmp_Ascii_Var(src1.mStr, src1_len,
                                 src2.mStr, src2_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());

#if 0    
    BOOST_MESSAGE(" src1=|" << s1 << "|" << 
                  " src2=|" << s2 << "|" <<
                  " expect=" << expected <<
                  " expect2=" << expected2 <<
                  " result=" << result);
#endif     
    BOOST_CHECK_EQUAL(result, expected);

    // check the exact opposite, even if equal
    int result2 = SqlStrCmp_Ascii_Var(src2.mStr, src2_len,
                                  src1.mStr, src1_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(result2 * -1, result);
    
    // force check of equal strings
    result = SqlStrCmp_Ascii_Var(src1.mStr, src1_len,
                             src1.mStr, src1_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK_EQUAL(result, 0);
    
}

    
void
SqlStringAsciiTest::testSqlStringCmp_Ascii_Var_DiffLen()
{
    int src1_storage, src2_storage, src1_len, src2_len;
    unsigned char startchar;

    for (src1_storage = 0; src1_storage <= MAXLEN; src1_storage++) {
        src1_len = src1_storage;
        for (src2_storage = 0; src2_storage <= MAXLEN; src2_storage++) {
            src2_len = src2_storage;
            // can't test w/ 0, confuses strcmp and/or std:string
            for (startchar = 1; startchar < 255; startchar++) {
                SqlStringBuffer src1(src1_storage, src1_len,
                                     0, src1_storage - src1_len, 
                                     'd', ' ');
                SqlStringBuffer src2(src2_storage, src2_len,
                                     0, src2_storage-src2_len,
                                     's', ' ');
                
                src1.patternfill(startchar, 1, 255);
                src2.patternfill(startchar, 1, 255);
                
                testSqlStringCmp_Ascii_Var_Helper(src1, src1_len,
                                        src2, src2_len);
            }
        }
    }
}


void
SqlStringAsciiTest::testSqlStringCmp_Ascii_Var_EqLen()
{
    int src1_storage, src2_storage, src1_len, src2_len, randX;
    
    // not much point large length, chances of 2 random strings being equal
    // are very low. test forces an equality check anyway.
    src1_storage = MAXCMPLEN;
    src1_len = src1_storage;
    src2_storage = src1_storage;
    src2_len = src1_storage;
    for (randX = 0; randX <= 65536; randX++) {
        SqlStringBuffer src1(src1_storage, src1_len,
                             0, src1_storage - src1_len, 
                             'd', ' ');
        SqlStringBuffer src2(src2_storage, src2_len,
                             0, src2_storage-src2_len,
                             's', ' ');
        
        // can't test w/ 0, confuses strcmp and/or std:string
        src1.randomize(1, 1, 255);
        src2.randomize(1, 1, 255);

        testSqlStringCmp_Ascii_Var_Helper(src1, src1_len,
                                src2, src2_len);
    }
}


void
SqlStringAsciiTest::testSqlStringLenBit_Ascii()
{
    int src_storage, src_len;
    int newlen;
    
    src_storage = MAXLEN;
    for (src_storage = 0; src_storage <= MAXLEN; src_storage++) {
        for (src_len = 0; src_len <= src_storage; src_len++) {
            SqlStringBuffer src(src_storage, src_len,
                                0, src_storage-src_len,
                                's', ' ');

            newlen = SqlStrLenBit_Ascii(src.mStr,
                                        src_len);
            BOOST_CHECK_EQUAL(newlen, src_len * 8);
            BOOST_CHECK(src.verify());

            newlen = SqlStrLenBit_Ascii(src.mStr,
                                       src_storage);
            BOOST_CHECK_EQUAL(newlen, src_storage * 8);
            BOOST_CHECK(src.verify());
        }
    }
}


void
SqlStringAsciiTest::testSqlStringLenChar_Ascii()
{
    int src_storage, src_len;
    int newlen;
    
    src_storage = MAXLEN;
    for (src_storage = 0; src_storage <= MAXLEN; src_storage++) {
        for (src_len = 0; src_len <= src_storage; src_len++) {
            SqlStringBuffer src(src_storage, src_len,
                                0, src_storage-src_len,
                                's', ' ');

            newlen = SqlStrLenChar_Ascii(src.mStr,
                                         src_len);
            BOOST_CHECK_EQUAL(newlen, src_len);
            BOOST_CHECK(src.verify());

            newlen = SqlStrLenChar_Ascii(src.mStr,
                                        src_storage);
            BOOST_CHECK_EQUAL(newlen, src_storage);
            BOOST_CHECK(src.verify());
        }
    }
}


void
SqlStringAsciiTest::testSqlStringLenOct_Ascii()
{
    int src_storage, src_len;
    int newlen;
    
    src_storage = MAXLEN;
    for (src_storage = 0; src_storage <= MAXLEN; src_storage++) {
        for (src_len = 0; src_len <= src_storage; src_len++) {
            SqlStringBuffer src(src_storage, src_len,
                                0, src_storage-src_len,
                                's', ' ');

            newlen = SqlStrLenOct_Ascii(src.mStr,
                                        src_len);
            BOOST_CHECK_EQUAL(newlen, src_len);
            BOOST_CHECK(src.verify());

            newlen = SqlStrLenOct_Ascii(src.mStr,
                                       src_storage);
            BOOST_CHECK_EQUAL(newlen, src_storage);
            BOOST_CHECK(src.verify());
        }
    }
}


void
SqlStringAsciiTest::testSqlStringOverlay_Ascii()
{
    int dst_storage, src_storage, src_len, over_storage, over_len;
    int position, length, lengthI;
    int exLeftLen, exMidLen, exRightLen;
    char *exLeftP, *exMidP, *exRightP;
    bool lenSpecified;
    bool caught;
    int newlen;

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
                            newlen = SqlStrOverlay_Ascii(dst.mStr, 
                                                         dst_storage,
                                                         src.mStr,
                                                         src_len,
                                                         over.mStr,
                                                         over_len,
                                                         position,
                                                         length,
                                                         lenSpecified);
                        } catch(const char *str) {
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
SqlStringAsciiTest::testSqlStringPos_Ascii()
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
                
                    foundpos  = SqlStrPos_Ascii(src.mStr, 
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

                        foundpos  = SqlStrPos_Ascii(src.mStr, 
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
SqlStringAsciiTest::testSqlStringSubStr_Ascii()
{
    int src_storage, src_len, dst_storage, newlen;
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
                                      "| dest_storage=" << dst_storage <<
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
                            newlen = SqlStrSubStr_Ascii(&resultP, dst_storage,
                                                        src.mStr, src_storage,
                                                        sub_start, sub_len, true);
                        } catch(const char *str) {
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
                                newlen = SqlStrSubStr_Ascii(&resultP, dst_storage,
                                                           src.mStr, src_storage,
                                                           sub_start, 0, false);
                            } catch(const char *str) {
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

void
SqlStringAsciiTest::testSqlStringToLower_Ascii()
{
    int dest_storage, dest_len, src_storage, src_len, randX;
    int newlen;
    bool caught;
    
    for (dest_storage = 0; dest_storage < MAXLEN; dest_storage++) {
        dest_len = dest_storage;
        for (src_storage = 0; src_storage < MAXLEN; src_storage++) {
            src_len = src_storage;
            for (randX = 0; randX < MAXRANDOM; randX++) {
                SqlStringBuffer dest(dest_storage, dest_len,
                                     0, 0,
                                     'd', ' ');
                SqlStringBuffer src(src_storage, src_len,
                                    0, src_storage-src_len,
                                    's', ' ');
                src.randomize('A');

                string save(src.mStr, src_len);
                string::iterator itr;
                char const *s;
                int count;

                // copy
                caught = false;
                try {
                    newlen = SqlStrToLower_Ascii(dest.mStr, dest_storage, src.mStr, src_len);
                } catch(const char *str) {
                    caught = true;
                    BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                    BOOST_CHECK(src_len > dest_storage);
                } catch(...) {
                    BOOST_CHECK(false);
                }
                if (!caught) {
                    BOOST_CHECK(src_len <= dest_storage);
                    BOOST_CHECK(src.verify());
                    BOOST_CHECK(dest.verify());
                    BOOST_CHECK_EQUAL(newlen, src_len);

                    itr = save.begin();
                    s = dest.mStr;
                    count = 0;
                    while(itr != save.end()) {
                        if (*itr >= 'A' && *itr <= 'Z') {
                            BOOST_CHECK_EQUAL(*s, (*itr + ('a' - 'A')));
                        } else {
                            BOOST_CHECK_EQUAL(*itr, *s);
                        }
                        s++;
                        itr++;
                        count++;
                    }
                    BOOST_CHECK_EQUAL(count, src_len);
                }
            }
        }
    }
}


void
SqlStringAsciiTest::testSqlStringToUpper_Ascii()
{
    int dest_storage, dest_len, src_storage, src_len, randX;
    int newlen;
    bool caught;
    

    for (dest_storage = 0; dest_storage < MAXLEN; dest_storage++) {
        dest_len = dest_storage;
        for (src_storage = 0; src_storage < MAXLEN; src_storage++) {
            src_len = src_storage;
            for (randX = 0; randX < MAXRANDOM; randX++) {
                SqlStringBuffer dest(dest_storage, dest_len,
                                     0, 0,
                                     'd', ' ');
                SqlStringBuffer src(src_storage, src_len,
                                    0, src_storage-src_len,
                                    's', ' ');
                src.randomize('a');

                string save(src.mStr, src_len);
                string::iterator itr;
                char const *s;
                int count;

                // copy
                caught = false;
                try {
                    newlen = SqlStrToUpper_Ascii(dest.mStr, dest_storage, src.mStr, src_len);
                } catch(const char *str) {
                    caught = true;
                    BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                    BOOST_CHECK(src_len > dest_storage);
                } catch(...) {
                    BOOST_CHECK(false);
                }
                if (!caught) {
                    BOOST_CHECK(src_len <= dest_storage);
                    BOOST_CHECK(src.verify());
                    BOOST_CHECK(dest.verify());
                    BOOST_CHECK_EQUAL(newlen, src_len);

                    itr = save.begin();
                    s = dest.mStr;
                    count = 0;

                    while(itr != save.end()) {
                        if (*itr >= 'a' && *itr <= 'z') {
                            BOOST_CHECK_EQUAL(*s, (*itr - ('a' - 'A')));
                        } else {
                            BOOST_CHECK_EQUAL(*itr, *s);
                        }
                        s++;
                        itr++;
                        count++;
                    }
                    BOOST_CHECK_EQUAL(count, src_len);
                }
            }
        }
    }
}

void
SqlStringAsciiTest::testSqlStringTrim_Ascii()
{
    int dst_storage, src_storage, src_len, leftpad, rightpad, randX, i;
    char padchar = ' ';
    char textchar = 's';
    bool caught;
    
    BOOST_REQUIRE(MAXLEN >= 5); // 2 pad + 1 text + 2 pad

    for (dst_storage = 0; dst_storage < MAXLEN; dst_storage++) {
        for (src_storage = 0; src_storage < dst_storage + 5; src_storage++) {
            for (src_len = 0; src_len <= src_storage; src_len++) {
                for (leftpad = 0; leftpad <= src_storage - src_len; leftpad++) {
                    rightpad = src_storage - (src_len + leftpad);
                    for (randX = 0; randX < MAXRANDOM; randX++) {
                        BOOST_REQUIRE(leftpad+rightpad+src_len == src_storage);
                        for (i = 0; i < 4; i++) {
                            int newsize;
                            int expectsize;
                            string resultByReference;
                            string resultCopy;
                            string expect;
                            SqlStringBuffer src(src_storage, src_len,
                                                leftpad, rightpad,
                                                textchar, padchar);
                            SqlStringBuffer dst(dst_storage, dst_storage,
                                                0, 0,
                                                textchar, padchar);
                            
                            int lefttrim, righttrim;
                            switch(i) {
                            case 0:
                                lefttrim = true;
                                righttrim = true;
                                expect.append(src_len, textchar);
                                expectsize = src_len;
                                break;
                            case 1:
                                lefttrim = true;
                                righttrim = false;
                                expect.append(src_len, textchar);
                                // if no text, everything is trimmed
                                if (src_len) expect.append(rightpad, padchar);
                                expectsize = src_len + (src_len ? rightpad : 0);
                                break;
                            case 2:
                                lefttrim = false;
                                righttrim = true;
                                // if no text, everything is trimmed
                                if (src_len) expect.append(leftpad, padchar);
                                expect.append(src_len, textchar);
                                expectsize = src_len + (src_len ? leftpad : 0);
                                break;
                            case 3:
                                lefttrim = false;
                                righttrim = false;
                                expect.append(leftpad, padchar);
                                expect.append(src_len, textchar);
                                expect.append(rightpad, padchar);
                                expectsize = src_len + leftpad + rightpad;
                                break;
                            }

                            // test copy implementation
                            caught = false;
                            try {
                                newsize = 
                                    SqlStrTrim_Ascii(dst.mStr,
                                                     dst_storage,
                                                     src.mStr,
                                                     src_len + leftpad + rightpad,
                                                     lefttrim,
                                                     righttrim);
                            } catch(const char *str) {
                                caught = true;
                                BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                                BOOST_CHECK(expectsize > dst_storage);
                            } catch(...) {
                                BOOST_CHECK(false);
                            }
                            
                            if (!caught) {
                                BOOST_CHECK(expectsize <= dst_storage);

                                BOOST_CHECK(src.verify());
                                BOOST_CHECK(dst.verify());
                                BOOST_CHECK_EQUAL(newsize, expectsize);
                                resultCopy = string(dst.mStr, newsize);

                                BOOST_CHECK(!resultCopy.compare(expect));
                                BOOST_CHECK(!expect.compare(resultCopy));

                            }


                            // test by reference
                            char const * start;
                            newsize = SqlStrTrim_Ascii(&start,
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
                        }
                    }
                }
            }
        }
    }
}


FENNEL_UNIT_TEST_SUITE(SqlStringAsciiTest);


    
    
        
