/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2009 The Eigenbase Project
// Copyright (C) 2004-2010 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
#include "fennel/calc/SqlString.h"
#include "fennel/common/TraceSource.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>

using namespace fennel;
using namespace std;


const int MAXLEN = 8;   // Must not be less than 5. Best >=7.
const int MAXRANDOM = 5;
const int MAXCMPLEN = 8;  // Must not be less than 3.
const int BUMPERLEN = 3;
const char BUMPERCH = '@';


// must support 0 length strings
class SqlStringTestGen
{
public:
    SqlStringTestGen(
        int storage,            // maximum size (column width) of string
        int size,             // size of text, excluding padding
        int leftpad = 0,      // pad left with this many chars
        int rightpad = 0,     // pad right with this many chars
        char text = 'x',     // fill text w/this
        char pad = ' ',      // pad w/this
        int leftBumper = BUMPERLEN,  // try to pick something unaligned...
        int rightBumper = BUMPERLEN)
        : mStorage(storage),
          mSize(size),
          mLeftPad(leftpad),
          mRightPad(rightpad),
          mLeftBump(leftBumper),
          mRightBump(rightBumper),
          mTotal(storage + leftBumper + rightBumper),
          mS(mTotal, BUMPERCH)
    {
        assert(leftBumper > 0);
        assert(rightBumper > 0);
        assert(storage == size + leftpad + rightpad);

        mLeftP = const_cast<char *>(mS.c_str()); // Too abusive of string()?
        mStr = mLeftP + mLeftBump;
        mRightP = mStr + mStorage;

        string padS(mStorage, pad);
        string textS(size, text);

        mS.replace(mLeftBump, mStorage, padS, 0, mStorage); // pad all first
        mS.replace(mLeftBump + mLeftPad, mSize, textS, 0, mSize);
    }

    bool
    verify()
    {
        string verS(mTotal, BUMPERCH);
        if (mS.compare(0, mLeftBump, verS, 0, mLeftBump)) {
            return false;
        }
        if (mS.compare(
            mLeftBump + mLeftPad + mSize + mRightPad,
            mRightBump, verS, 0, mRightBump))
        {
            return false;
        }
        return true;
    }

    void
    randomize(
        unsigned char start = 'A',
        unsigned char lower = ' ',
        unsigned char upper = '~')
    {
        patternfill(start, lower, upper);
        string r(mStr, mSize);
        random_shuffle(r.begin(), r.end());
        mS.replace(mLeftBump + mLeftPad, mSize, r);
    }

    void
    patternfill(
        unsigned char start = 'A',
        unsigned char lower = ' ',
        unsigned char upper = '~')
    {
        uint c; // deal with overflow easier than char
        int toGen = mSize;

        string r;

        c = start;
        while (toGen) {
            r.push_back(static_cast<unsigned char>(c));
            toGen--;
            if (++c > upper) {
                c = lower;
            }
        }
        mS.replace(mLeftBump + mLeftPad, mSize, r);
    }


    char * mStr;           // valid string start. (includes left padding)
    char * mRightP;   // right bumper start. valid string ends 1 before here
    char * mLeftP;         // left bumper start.
    const int mStorage;    // maximum size (column width) of string
    const int mSize;       // size of string
    const int mLeftPad;    // length of left padding
    const int mRightPad;   // length of right padding
    const int mLeftBump;   // length of left bumper
    const int mRightBump;  // length of right bumper
    const int mTotal;      // size of string + bumpers
    string mS;

private:

    string
    vectortostring(vector<char> &v)
    {
        string s;
        vector<char>::iterator i = v.begin();
        while (i != v.end()) {
            s.push_back(*i);
            i++;
        }
        return s;
    }
};


class SqlStringTest : virtual public TestBase, public TraceSource
{
    void testSqlStringClass();

    void testSqlStringAsciiCatF();
    void testSqlStringAsciiCatV();
    void testSqlStringAsciiCatV2();
    void testSqlStringAsciiCmpFDiffLen();
    void testSqlStringAsciiCmpFEqLen();
    void testSqlStringAsciiCmpVDiffLen();
    void testSqlStringAsciiCmpVEqLen();
    void testSqlStringAsciiLenBit();
    void testSqlStringAsciiLenChar();
    void testSqlStringAsciiLenOct();
    void testSqlStringAsciiOverlay();
    void testSqlStringAsciiPos();
    void testSqlStringAsciiSubStr();
    void testSqlStringAsciiToLower();
    void testSqlStringAsciiToUpper();
    void testSqlStringAsciiTrim();

    void testSqlStringAsciiCmpVHelper(
        SqlStringTestGen &src1,
        int src1_len,
        SqlStringTestGen &src2,
        int src2_len);
    void testSqlStringAsciiCmpFHelper(
        SqlStringTestGen &src1,
        int src1_storage,
        int src1_len,
        SqlStringTestGen &src2,
        int src2_storage,
        int src2_len);
    int testSqlStringNormalizeLexicalCmp(int v);

public:
    explicit SqlStringTest()
        : TraceSource(shared_from_this(), "SqlStringTest")
    {
        srand(time(NULL));
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringClass);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiCatF);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiCatV2);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiCatV);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiCmpFDiffLen);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiCmpFEqLen);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiCmpVDiffLen);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiCmpVEqLen);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiLenBit);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiLenChar);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiLenOct);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiOverlay);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiPos);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiSubStr);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiToLower);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiToUpper);
        FENNEL_UNIT_TEST_CASE(SqlStringTest, testSqlStringAsciiTrim);
    }

    virtual ~SqlStringTest()
    {
    }
};

void
SqlStringTest::testSqlStringClass()
{
    int storage, size, leftpad, rightpad;
    int leftbump = 2;
    int rightbump = 2;
    int k;

    for (storage = 0; storage <= 5; storage++) {
        for (size = 0; size <= storage; size++) {
            for (leftpad = 0; leftpad <= storage - size; leftpad++) {
                rightpad = (storage - size) - leftpad;

                SqlStringTestGen t(
                    storage, size,
                    leftpad, rightpad,
                    'x', ' ',
                    leftbump, rightbump);

                BOOST_CHECK_EQUAL(t.mStorage, storage);
                BOOST_CHECK_EQUAL(t.mSize, size);
                BOOST_CHECK_EQUAL(t.mLeftPad, leftpad);
                BOOST_CHECK_EQUAL(t.mRightPad, rightpad);
                BOOST_CHECK_EQUAL(
                    static_cast<int>(t.mS.size()),
                    storage + leftbump + rightbump);

                BOOST_CHECK(t.verify());

                char *p = t.mLeftP;
                // left bumper
                for (k = 0; k < leftbump; k++) {
                    BOOST_CHECK_EQUAL(*(p++), BUMPERCH);
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
                    BOOST_CHECK_EQUAL(*(p++), BUMPERCH);
                }
                BOOST_CHECK_EQUAL(
                    static_cast<int>(p - t.mLeftP),
                    storage + leftbump + rightbump);

                BOOST_CHECK(t.verify());

                for (k = 0; k < size; k++) {
                    *(t.mStr + k) = '0' + (k % 10);
                }
                BOOST_CHECK(t.verify());

                *(t.mLeftP) = 'X';
                BOOST_CHECK(!t.verify());
                *(t.mLeftP) = BUMPERCH;
                BOOST_CHECK(t.verify());

                *(t.mStr - 1) = 'X';
                BOOST_CHECK(!t.verify());
                *(t.mStr - 1) = BUMPERCH;
                BOOST_CHECK(t.verify());

                *(t.mRightP) = 'X';
                BOOST_CHECK(!t.verify());
                *(t.mRightP) = BUMPERCH;
                BOOST_CHECK(t.verify());

                *(t.mRightP + t.mRightBump - 1) = 'X';
                BOOST_CHECK(!t.verify());
                *(t.mRightP + t.mRightBump - 1) = BUMPERCH;
                BOOST_CHECK(t.verify());

                t.randomize();
                BOOST_CHECK(t.verify());
            }
        }
    }
}


// Test catting 3 fixed width strings together as proof-of-concept
void
SqlStringTest::testSqlStringAsciiCatF()
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
                        for (src3_storage = 0; src3_storage < MAXLEN;
                             src3_storage++)
                        {
                            for (src3_len = 0; src3_len <= src3_storage;
                                 src3_len++)
                            {
                                SqlStringTestGen dst(
                                    dst_storage, 0,
                                    0, dst_storage,
                                    'd', ' ');
                                SqlStringTestGen src1(
                                    src1_storage, src1_len,
                                    0, src1_storage - src1_len,
                                    '1', ' ');
                                SqlStringTestGen src2(
                                    src2_storage, src2_len,
                                    0, src2_storage - src2_len,
                                    '2', ' ');
                                SqlStringTestGen src3(
                                    src3_storage, src3_len,
                                    0, src3_storage - src3_len,
                                    '3', ' ');

                                caught = false;
                                try {
                                    newlen = SqlStrAsciiCat(
                                        dst.mStr, dst_storage,
                                        src1.mStr, src1_storage,
                                        src2.mStr, src2_storage);
                                } catch (SqlStateInfo const &info) {
                                    const char *str = info.str().c_str();
                                    caught = true;
                                    BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                                    BOOST_CHECK(
                                        src1_storage + src2_storage
                                        > dst_storage);
                                    BOOST_CHECK(dst.verify());
                                    BOOST_CHECK(src1.verify());
                                    BOOST_CHECK(src2.verify());
                                } catch (...) {
                                    BOOST_CHECK(false);
                                }
                                if (!caught) {
                                    BOOST_CHECK(
                                        src1_storage + src2_storage
                                        <= dst_storage);
                                    BOOST_CHECK(dst.verify());
                                    BOOST_CHECK(src1.verify());
                                    BOOST_CHECK(src2.verify());

                                    caught = false;
                                    try {
                                        newlen = SqlStrAsciiCat(
                                            dst.mStr,
                                            dst_storage,
                                            newlen,
                                            src3.mStr,
                                            src3_storage);
                                    } catch (SqlStateInfo const &info) {
                                        const char *str = info.str().c_str();
                                        caught = true;
                                        BOOST_CHECK_EQUAL(
                                            strcmp(str, "22001"),
                                            0);
                                        BOOST_CHECK(
                                            (src1_storage
                                             + src2_storage
                                             + src3_storage)
                                            > dst_storage);
                                        BOOST_CHECK(dst.verify());
                                        BOOST_CHECK(src1.verify());
                                        BOOST_CHECK(src2.verify());
                                        BOOST_CHECK(src3.verify());
                                    } catch (...) {
                                        BOOST_CHECK(false);
                                    }
                                    if (!caught) {
                                        BOOST_CHECK(dst.verify());
                                        BOOST_CHECK(src1.verify());
                                        BOOST_CHECK(src2.verify());
                                        BOOST_CHECK(src3.verify());
                                        BOOST_CHECK_EQUAL(
                                            newlen,
                                            (src1_storage
                                             + src2_storage
                                             + src3_storage));

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
SqlStringTest::testSqlStringAsciiCatV2()
{
    int src1_storage, src2_storage, dst_storage, src1_len, src2_len;
    int newlen;
    bool caught;

    for (dst_storage = 0; dst_storage < MAXLEN; dst_storage++) {
        for (src1_storage = 0; src1_storage < MAXLEN; src1_storage++) {
            for (src1_len = 0; src1_len <= src1_storage; src1_len++) {
                for (src2_storage = 0; src2_storage < MAXLEN; src2_storage++) {
                    for (src2_len = 0; src2_len <= src2_storage; src2_len++) {
                        SqlStringTestGen dst(
                            dst_storage, 0,
                            0, dst_storage,
                            'd', ' ');
                        SqlStringTestGen src1(
                            src1_storage, src1_len,
                            0, src1_storage - src1_len,
                            's', ' ');
                        SqlStringTestGen src2(
                            src2_storage, src2_len,
                            0, src2_storage - src2_len,
                            'S', ' ');

                        caught = false;
                        try {
                            newlen = SqlStrAsciiCat(
                                dst.mStr,
                                dst_storage,
                                src1.mStr,
                                src1_len,
                                src2.mStr,
                                src2_len);
                        } catch (SqlStateInfo const &info) {
                            const char *str = info.str().c_str();
                            caught = true;
                            BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                            BOOST_CHECK(src1_len + src2_len > dst_storage);
                        } catch (...) {
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
SqlStringTest::testSqlStringAsciiCatV()
{
    int src_storage, dst_storage, src_len, dst_len;
    int newlen;
    bool caught;

    for (dst_storage = 0; dst_storage < MAXLEN; dst_storage++) {
        for (dst_len = 0; dst_len <= dst_storage; dst_len++) {
            for (src_storage = 0; src_storage < MAXLEN; src_storage++) {
                for (src_len = 0; src_len <= src_storage; src_len++) {
                    SqlStringTestGen dst(
                        dst_storage, dst_len,
                        0, dst_storage - dst_len,
                        'd', ' ');
                    SqlStringTestGen src(
                        src_storage, src_len,
                        0, src_storage - src_len,
                        's', ' ');
                    caught = false;
                    try {
                        newlen = SqlStrAsciiCat(
                            dst.mStr,
                            dst_storage,
                            dst_len,
                            src.mStr,
                            src_len);
                    } catch (SqlStateInfo const &info) {
                        const char *str = info.str().c_str();
                        caught = true;
                        BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                        BOOST_CHECK(src_len + dst_len > dst_storage);
                    } catch (...) {
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
SqlStringTest::testSqlStringNormalizeLexicalCmp(int v)
{
    if (v < 0) {
        return -1;
    }
    if (v > 0) {
        return 1;
    }
    return 0;
}



void
SqlStringTest::testSqlStringAsciiCmpFHelper(
    SqlStringTestGen &src1,
    int src1_storage,
    int src1_len,
    SqlStringTestGen &src2,
    int src2_storage,
    int src2_len)
{
    int result;

    string s1(src1.mStr, src1_len);
    string s2(src2.mStr, src2_len);

    // It is possible that test string ends with a space. Remove it.
    s1.erase(s1.find_last_not_of(" ") + 1);
    s2.erase(s2.find_last_not_of(" ") + 1);

    int expected = testSqlStringNormalizeLexicalCmp(s1.compare(s2));
    char const * const s1p = s1.c_str();
    char const * const s2p = s2.c_str();
    int expected2 = testSqlStringNormalizeLexicalCmp(strcmp(s1p, s2p));
    BOOST_CHECK_EQUAL(expected, expected2);

    result = SqlStrAsciiCmpF(
        src1.mStr, src1_storage,
        src2.mStr, src2_storage);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());

#if 0
    BOOST_MESSAGE(
        " src1=|" << s1 << "|"
        << " src2=|" << s2 << "|"
        << " expect=" << expected
        << " expect2=" << expected2
        << " result=" << result);
#endif
    BOOST_CHECK_EQUAL(result, expected);

    // check the exact opposite, even if equal
    int result2 = SqlStrAsciiCmpF(
        src2.mStr, src2_storage,
        src1.mStr, src1_storage);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(result2 * -1, result);

    // force check of equal strings
    result = SqlStrAsciiCmpF(
        src1.mStr, src1_storage,
        src1.mStr, src1_storage);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK_EQUAL(result, 0);

}


void
SqlStringTest::testSqlStringAsciiCmpFDiffLen()
{
    int src1_storage, src2_storage, src1_len, src2_len;
    unsigned char startchar;

    for (src1_storage = 0; src1_storage <= MAXLEN; src1_storage++) {
        for (src1_len = 0; src1_len < src1_storage; src1_len++) {
            for (src2_storage = 0; src2_storage <= MAXLEN; src2_storage++) {
                for (src2_len = 0; src2_len < src2_storage; src2_len++) {
                    // can't test w/ 0, confuses strcmp and/or std:string
                    for (startchar = 1; startchar < 255; startchar++) {
                        SqlStringTestGen src1(
                            src1_storage, src1_len,
                            0, src1_storage - src1_len,
                            'd', ' ');
                        SqlStringTestGen src2(
                            src2_storage, src2_len,
                            0, src2_storage - src2_len,
                            's', ' ');

                        src1.patternfill(startchar, 1, 255);
                        src2.patternfill(startchar, 1, 255);

                        testSqlStringAsciiCmpFHelper(
                            src1, src1_storage, src1_len,
                            src2, src2_storage, src2_len);
                    }
                }
            }
        }
    }
}


void
SqlStringTest::testSqlStringAsciiCmpFEqLen()
{
    int src1_storage, src2_storage, src1_len, src2_len, randX;

    // not much point large length, chances of 2 random strings being equal
    // are very low. test forces an equality check anyway.
    src1_storage = MAXCMPLEN;
    src2_storage = MAXCMPLEN;
    for (src1_len = 0; src1_len < src1_storage; src1_len++) {
        src2_len = src1_len;
        for (randX = 0; randX <= 65536; randX++) {
            SqlStringTestGen src1(
                src1_storage, src1_len,
                0, src1_storage - src1_len,
                'd', ' ');
            SqlStringTestGen src2(
                src2_storage, src2_len,
                0, src2_storage - src2_len,
                's', ' ');

            // can't test w/ 0, confuses strcmp and/or std:string
            src1.randomize(1, 1, 255);
            src2.randomize(1, 1, 255);

            testSqlStringAsciiCmpFHelper(
                src1, src1_storage, src1_len,
                src2, src2_storage, src2_len);
        }
    }
}


void
SqlStringTest::testSqlStringAsciiCmpVHelper(
    SqlStringTestGen &src1,
    int src1_len,
    SqlStringTestGen &src2,
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


    result = SqlStrAsciiCmpV(
        src1.mStr, src1_len,
        src2.mStr, src2_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());

#if 0
    BOOST_MESSAGE(
        " src1=|" << s1 << "|"
        << " src2=|" << s2 << "|"
        << " expect=" << expected
        << " expect2=" << expected2
        << " result=" << result);
#endif
    BOOST_CHECK_EQUAL(result, expected);

    // check the exact opposite, even if equal
    int result2 = SqlStrAsciiCmpV(
        src2.mStr, src2_len,
        src1.mStr, src1_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK(src2.verify());
    BOOST_CHECK_EQUAL(result2 * -1, result);

    // force check of equal strings
    result = SqlStrAsciiCmpV(
        src1.mStr, src1_len,
        src1.mStr, src1_len);
    BOOST_CHECK(src1.verify());
    BOOST_CHECK_EQUAL(result, 0);

}


void
SqlStringTest::testSqlStringAsciiCmpVDiffLen()
{
    int src1_storage, src2_storage, src1_len, src2_len;
    unsigned char startchar;

    for (src1_storage = 0; src1_storage <= MAXLEN; src1_storage++) {
        src1_len = src1_storage;
        for (src2_storage = 0; src2_storage <= MAXLEN; src2_storage++) {
            src2_len = src2_storage;
            // can't test w/ 0, confuses strcmp and/or std:string
            for (startchar = 1; startchar < 255; startchar++) {
                SqlStringTestGen src1(
                    src1_storage, src1_len,
                    0, src1_storage - src1_len,
                    'd', ' ');
                SqlStringTestGen src2(
                    src2_storage, src2_len,
                    0, src2_storage - src2_len,
                    's', ' ');

                src1.patternfill(startchar, 1, 255);
                src2.patternfill(startchar, 1, 255);

                testSqlStringAsciiCmpVHelper(
                    src1, src1_len,
                    src2, src2_len);
            }
        }
    }
}


void
SqlStringTest::testSqlStringAsciiCmpVEqLen()
{
    int src1_storage, src2_storage, src1_len, src2_len, randX;

    // not much point large length, chances of 2 random strings being equal
    // are very low. test forces an equality check anyway.
    src1_storage = MAXCMPLEN;
    src1_len = src1_storage;
    src2_storage = src1_storage;
    src2_len = src1_storage;
    for (randX = 0; randX <= 65536; randX++) {
        SqlStringTestGen src1(
            src1_storage, src1_len,
            0, src1_storage - src1_len,
            'd', ' ');
        SqlStringTestGen src2(
            src2_storage, src2_len,
            0, src2_storage - src2_len,
            's', ' ');

        // can't test w/ 0, confuses strcmp and/or std:string
        src1.randomize(1, 1, 255);
        src2.randomize(1, 1, 255);

        testSqlStringAsciiCmpVHelper(
            src1, src1_len,
            src2, src2_len);
    }
}


void
SqlStringTest::testSqlStringAsciiLenBit()
{
    int src_storage, src_len;
    int newlen;

    src_storage = MAXLEN;
    for (src_storage = 0; src_storage <= MAXLEN; src_storage++) {
        for (src_len = 0; src_len <= src_storage; src_len++) {
            SqlStringTestGen src(
                src_storage, src_len,
                0, src_storage - src_len,
                's', ' ');

            newlen = SqlStrAsciiLenBit(
                src.mStr,
                src_len);
            BOOST_CHECK_EQUAL(newlen, src_len * 8);
            BOOST_CHECK(src.verify());

            newlen = SqlStrAsciiLenBit(
                src.mStr,
                src_storage);
            BOOST_CHECK_EQUAL(newlen, src_storage * 8);
            BOOST_CHECK(src.verify());
        }
    }
}


void
SqlStringTest::testSqlStringAsciiLenChar()
{
    int src_storage, src_len;
    int newlen;

    src_storage = MAXLEN;
    for (src_storage = 0; src_storage <= MAXLEN; src_storage++) {
        for (src_len = 0; src_len <= src_storage; src_len++) {
            SqlStringTestGen src(
                src_storage, src_len,
                0, src_storage - src_len,
                's', ' ');

            newlen = SqlStrAsciiLenChar(
                src.mStr,
                src_len);
            BOOST_CHECK_EQUAL(newlen, src_len);
            BOOST_CHECK(src.verify());

            newlen = SqlStrAsciiLenChar(
                src.mStr,
                src_storage);
            BOOST_CHECK_EQUAL(newlen, src_storage);
            BOOST_CHECK(src.verify());
        }
    }
}


void
SqlStringTest::testSqlStringAsciiLenOct()
{
    int src_storage, src_len;
    int newlen;

    src_storage = MAXLEN;
    for (src_storage = 0; src_storage <= MAXLEN; src_storage++) {
        for (src_len = 0; src_len <= src_storage; src_len++) {
            SqlStringTestGen src(
                src_storage, src_len,
                0, src_storage - src_len,
                's', ' ');

            newlen = SqlStrAsciiLenOct(
                src.mStr,
                src_len);
            BOOST_CHECK_EQUAL(newlen, src_len);
            BOOST_CHECK(src.verify());

            newlen = SqlStrAsciiLenOct(
                src.mStr,
                src_storage);
            BOOST_CHECK_EQUAL(newlen, src_storage);
            BOOST_CHECK(src.verify());
        }
    }
}


void
SqlStringTest::testSqlStringAsciiOverlay()
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
                        BOOST_MESSAGE(
                            " dst_storage=" << dst_storage
                            << " src_storage=" << src_storage
                            << " over_storage=" << over_storage
                            << " pos=" << position
                            << " length=" << length
                            << " spec=" << lenSpecified);
#endif
                        SqlStringTestGen dst(
                            dst_storage, dst_storage,
                            0, 0,
                            'd', ' ');
                        SqlStringTestGen src(
                            src_storage, src_len,
                            0, src_storage - src_len,
                            's', ' ');
                        SqlStringTestGen over(
                            over_storage, over_len,
                            0, over_storage - over_len,
                            'o', ' ');

                        src.patternfill('a', 'a', 'z');
                        over.patternfill('A', 'A', 'Z');

                        // ex* vars are 0-indexed. for loops are 1-indexed
                        exLeftP = src.mStr;
                        if (position >= 1 && src_len >= 1) {
                            exLeftLen = position - 1;  // 1-idx -> 0-idx
                            if (exLeftLen > src_len) {
                                exLeftLen = src_len;
                            }
                        } else {
                            exLeftLen = 0;
                        }

                        exMidP = over.mStr;
                        exMidLen = over_len;

                        exRightLen = src_len - (exLeftLen + length);
                        if (exRightLen < 0) {
                            exRightLen = 0;
                        }
                        exRightP = exLeftP + (src_len - exRightLen);

                        string expect(exLeftP, exLeftLen);
                        expect.append(exMidP, exMidLen);
                        expect.append(exRightP, exRightLen);

                        caught = false;
                        try {
                            newlen = SqlStrAsciiOverlay(
                                dst.mStr,
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
                                BOOST_CHECK(
                                    position < 1
                                    || (lenSpecified && length < 1));
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
SqlStringTest::testSqlStringAsciiPos()
{
    int src_storage, find_start, find_len, randX;
    int alter_char;

    int foundpos;

    for (src_storage = 0; src_storage < MAXLEN; src_storage++) {
        for (randX = 0; randX < MAXRANDOM; randX++) {
            SqlStringTestGen src(
                src_storage, src_storage,
                0, 0,
                's', ' ');
            src.randomize('a', 'a', 'z');

            // find all possible valid substrings
            for (find_start = 0; find_start <= src_storage; find_start++) {
                for (find_len = 0; find_len <= src_storage - find_start;
                     find_len++)
                {
                    string validsubstr(src.mStr + find_start, find_len);
                    SqlStringTestGen find(
                        find_len, find_len,
                        0, 0,
                        'X', ' ');
                    memcpy(find.mStr, validsubstr.c_str(), find_len);

                    foundpos  = SqlStrAsciiPos(
                        src.mStr,
                        src_storage,
                        find.mStr,
                        find_len);
                    BOOST_CHECK(src.verify());
                    BOOST_CHECK(find.verify());

                    if (find_len) {
                        // foundpos is 1-indexed. find_start is 0-indexed.
                        BOOST_CHECK_EQUAL(foundpos, find_start + 1);
                    } else {
                        BOOST_CHECK_EQUAL(
                            foundpos, static_cast<int>(1));  // Case A.
                    }

                    // alter valid substring to prevent match
                    for (alter_char = 0; alter_char < find_len; alter_char++) {
                        char save = *(find.mStr + alter_char);
                        // 'X' not between 'a' and 'z'
                        *(find.mStr + alter_char) = 'X';

                        foundpos  = SqlStrAsciiPos(
                            src.mStr,
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
SqlStringTest::testSqlStringAsciiSubStr()
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
                for (sub_start = -3; sub_start <= 3 + src_storage;
                     sub_start++)
                {
                    for (sub_len = -3; sub_len <= 3 + src_storage; sub_len++) {
                        SqlStringTestGen dst(
                            dst_storage, dst_storage,
                            0, 0,
                            'd', ' ');
                        SqlStringTestGen src(
                            src_storage, src_len,
                            0, src_storage - src_len,
                            's', ' ');
                        src.randomize();
#if 0
                        BOOST_MESSAGE(
                            "src =|" << src.mLeftP
                            << "| dest_storage=" << dst_storage
                            << " src_storage=" << src_storage
                            << " src_len=" << src_len
                            << " sub_start=" << sub_start
                            << " sub_len=" << sub_len);
#endif
                        int exsubstart = sub_start;
                        int exlen = sub_len;
                        if (exsubstart < 1) {
                            // will grab fewer characters
                            exlen += (exsubstart - 1);
                        }
                        exsubstart--;                       // convert index
                        if (exsubstart < 0) {
                            exsubstart = 0; // clean up for std::string
                        }
                        if (exlen < 0) {
                            exlen = 0;           // clean up for std::string
                        }

                        if (exsubstart + exlen > src_storage) {
                            if (exsubstart > src_storage) {
                                exlen = 0;
                            } else {
                                exlen = src_storage - exsubstart;
                            }
                        }
                        if (exsubstart < 0) {
                            exsubstart = 0; // clean up for std::string
                        }
                        if (exlen < 0) {
                            exlen = 0;           // clean up for std::string
                        }

                        string expect(src.mStr + exsubstart, exlen);

                        caught = false;
                        try {
                            newlen = SqlStrAsciiSubStr(
                                &resultP, dst_storage,
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
                        if (sub_start > 0 && sub_len > 0
                            && sub_start + sub_len - 1 > src_storage)
                        {
                            caught = false;
                            try {
                                newlen = SqlStrAsciiSubStr(
                                    &resultP, dst_storage,
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
                                // BOOST_MESSAGE(
                                //     " len=" << sub_len
                                //     << " start=" << sub_start);

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
SqlStringTest::testSqlStringAsciiToLower()
{
    int dest_storage, dest_len, src_storage, src_len, randX;
    int newlen;
    bool caught;

    for (dest_storage = 0; dest_storage < MAXLEN; dest_storage++) {
        dest_len = dest_storage;
        for (src_storage = 0; src_storage < MAXLEN; src_storage++) {
            src_len = src_storage;
            for (randX = 0; randX < MAXRANDOM; randX++) {
                SqlStringTestGen dest(
                    dest_storage, dest_len,
                    0, 0,
                    'd', ' ');
                SqlStringTestGen src(
                    src_storage, src_len,
                    0, src_storage - src_len,
                    's', ' ');
                src.randomize('A');

                string save(src.mStr, src_len);
                string::iterator itr;
                char const *s;
                int count;

                // copy
                caught = false;
                try {
                    newlen = SqlStrAsciiToLower(
                        dest.mStr, dest_storage, src.mStr, src_len);
                } catch (const char *str) {
                    caught = true;
                    BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                    BOOST_CHECK(src_len > dest_storage);
                } catch (...) {
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
                    while (itr != save.end()) {
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
SqlStringTest::testSqlStringAsciiToUpper()
{
    int dest_storage, dest_len, src_storage, src_len, randX;
    int newlen;
    bool caught;


    for (dest_storage = 0; dest_storage < MAXLEN; dest_storage++) {
        dest_len = dest_storage;
        for (src_storage = 0; src_storage < MAXLEN; src_storage++) {
            src_len = src_storage;
            for (randX = 0; randX < MAXRANDOM; randX++) {
                SqlStringTestGen dest(
                    dest_storage, dest_len,
                    0, 0,
                    'd', ' ');
                SqlStringTestGen src(
                    src_storage, src_len,
                    0, src_storage - src_len,
                    's', ' ');
                src.randomize('a');

                string save(src.mStr, src_len);
                string::iterator itr;
                char const *s;
                int count;

                // copy
                caught = false;
                try {
                    newlen = SqlStrAsciiToUpper(
                        dest.mStr, dest_storage, src.mStr, src_len);
                } catch (const char *str) {
                    caught = true;
                    BOOST_CHECK_EQUAL(strcmp(str, "22001"), 0);
                    BOOST_CHECK(src_len > dest_storage);
                } catch (...) {
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

                    while (itr != save.end()) {
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
SqlStringTest::testSqlStringAsciiTrim()
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
                        BOOST_REQUIRE(
                            leftpad + rightpad + src_len == src_storage);
                        for (i = 0; i < 4; i++) {
                            int newsize;
                            int expectsize;
                            string resultByReference;
                            string resultCopy;
                            string expect;
                            SqlStringTestGen src(
                                src_storage, src_len,
                                leftpad, rightpad,
                                textchar, padchar);
                            SqlStringTestGen dst(
                                dst_storage, dst_storage,
                                0, 0,
                                textchar, padchar);

                            int lefttrim, righttrim;
                            switch (i) {
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
                                if (src_len) {
                                    expect.append(rightpad, padchar);
                                }
                                expectsize = src_len + (src_len ? rightpad : 0);
                                break;
                            case 2:
                                lefttrim = false;
                                righttrim = true;
                                // if no text, everything is trimmed
                                if (src_len) {
                                    expect.append(leftpad, padchar);
                                }
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
                                    SqlStrAsciiTrim(
                                        dst.mStr,
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
                            newsize = SqlStrAsciiTrim(
                                &start,
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


FENNEL_UNIT_TEST_SUITE(SqlStringTest);

// End SqlStringTestAscii.cpp
