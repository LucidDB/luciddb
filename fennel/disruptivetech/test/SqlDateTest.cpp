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
#include "fennel/disruptivetech/calc/SqlDate.h"
#include "fennel/common/TraceSource.h"
#include "fennel/disruptivetech/test/SqlStringBuffer.h"
#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>
#include <iostream>

#ifdef HAVE_ICU
#include <unicode/unistr.h>
#include <unicode/uloc.h>
#endif

using namespace fennel;
using namespace std;


static const int MAXLEN = 8;   // Must not be less than 5. Best >=7.
static const int MAXRANDOM = 5;
static const int MAXCMPLEN = 8;  // Must not be less than 3.


static int64_t const ticks_per_day = boost::posix_time::ptime::time_rep_type::frac_sec_per_day();
static int64_t const ticks_per_year = ticks_per_day * 365LL;
static int64_t const ticks_per_month = ticks_per_day * 31LL;
static int64_t const ticks_per_hour = ticks_per_day/24;
static int64_t const ticks_per_minute = ticks_per_hour/60;
static int64_t const ticks_per_sec = ticks_per_minute/60;

enum SqlStrToDateConvAction {
    StrToDate,
    StrToTime,
    StrToTimestamp
};

class SqlDateTest : virtual public TestBase, public TraceSource
{

    void testSqlDateToStr_Ascii();
    void testSqlTimeToStr_Ascii();
    void appendCharsToUCS2LikeString(string& str,
                                     int number,
                                     char character);

    void testSqlStrToDate_Ascii_Helper(SqlStrToDateConvAction action,
                                       uint64_t value,
                                       char const * const src,
                                       int len,
                                       bool errorExpected);
    void testSqlStrToDate_Ascii();
    
    void testLocalTime();
    
#ifdef HAVE_ICU
    string UnicodeToPrintable(const UnicodeString &s);
#endif

    
public:
    explicit SqlDateTest()
        : TraceSource(shared_from_this(),"SqlDateTest")
    {
        srand(time(NULL));
        FENNEL_UNIT_TEST_CASE(SqlDateTest, testSqlDateToStr_Ascii);
        FENNEL_UNIT_TEST_CASE(SqlDateTest, testSqlTimeToStr_Ascii);
        //        FENNEL_UNIT_TEST_CASE(SqlDateTest, testSqlTimeStampToStr_Ascii);
        FENNEL_UNIT_TEST_CASE(SqlDateTest, testSqlStrToDate_Ascii);
        //        FENNEL_UNIT_TEST_CASE(SqlDateTest, testSqlStrToTime_Ascii);
        //        FENNEL_UNIT_TEST_CASE(SqlDateTest, testSqlStrToTimestamp_Ascii);
        FENNEL_UNIT_TEST_CASE(SqlDateTest, testLocalTime);
    }
    
    virtual ~SqlDateTest()
    {
    }
};

#ifdef HAVE_ICU
// Note: Assumes that only ASCII chars are represented
string
SqlDateTest::UnicodeToPrintable(const UnicodeString &s) {
    ostringstream o;
    int32_t i, length;
    char tmp;
    
    // output the code units (not code points)
    length = s.length();
    for(i=0; i<length; ++i) {
        tmp = s.charAt(i) & 0xff;
        o << i << "=" << tmp << " | ";
    }
    return o.str();
}
#endif

// build these strings by hand, not using ICU, so we're also
// testing that ICU is integrated and working correctly.
void
SqlDateTest::appendCharsToUCS2LikeString(string& str,
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
SqlDateTest::testSqlDateToStr_Ascii()
{
    int storage; 
    int leftbump = 2;
    int rightbump = 2;

    
    
    SqlStringBuffer s1(10, 10,
                      0, 0,
                      'x', ' ', 
                      leftbump, rightbump);

    storage = 10;
    SqlDateToStr<1,1,SQLDATE>(s1.mStr, storage, 0);
    BOOST_CHECK(s1.verify());
    SqlDateToStr<1,1,SQLDATE>(s1.mStr, storage, ticks_per_day);
    BOOST_CHECK(s1.verify());
    SqlDateToStr<1,1,SQLDATE>(s1.mStr, storage, ticks_per_month);
    BOOST_CHECK(s1.verify());

    int size, leftpad, rightpad; 
    for (storage = 5; storage <= 15; storage++) {
        for (size = 0; size <= storage; size++) {
            for (leftpad = 0; leftpad <= storage-size; leftpad++) {
                rightpad = (storage-size) - leftpad;

                SqlStringBuffer t(storage, size,
                                  leftpad, rightpad,
                                  'x', ' ', 
                                  leftbump, rightbump);

                bool caught = false;
                try {
                    SqlDateToStr<1,1,SQLDATE>(t.mStr, storage, ticks_per_year + size*ticks_per_month + storage*ticks_per_day);
                } catch(const char *str) {
                    caught = true;
                    BOOST_CHECK_EQUAL(strcmp(str,"22001"),0);
                    BOOST_CHECK(t.verify());
                    BOOST_CHECK(storage < 10);
                } catch(...) {
                    BOOST_CHECK(false);
                }
                if (!caught) {
                    BOOST_CHECK(t.verify());
                    //                    cout << t.mStr << endl;
                }

            }
        }
    } 
}

void
SqlDateTest::testSqlTimeToStr_Ascii()
{
    int storage ;
    int leftbump = 2;
    int rightbump = 2;

    SqlStringBuffer s1(10, 10,
                      0, 0,
                      'x', ' ', 
                      leftbump, rightbump);

    storage = 10;
    SqlDateToStr<1,1,SQLTIME>(s1.mStr, storage, 0);
    BOOST_CHECK(s1.verify());
    SqlDateToStr<1,1,SQLTIME>(s1.mStr, storage, ticks_per_hour);
    BOOST_CHECK(s1.verify());
    SqlDateToStr<1,1,SQLTIME>(s1.mStr, storage, 13LL * ticks_per_hour);
    BOOST_CHECK(s1.verify());
    SqlDateToStr<1,1,SQLTIME>(s1.mStr, storage, 1000*57601000LL);
    BOOST_CHECK(s1.verify());
    //    cout << s1.mStr << endl;
    
    int size, leftpad, rightpad; 
    for (storage = 5; storage <= 15; storage++) {
        for (size = 0; size <= storage; size++) {
            for (leftpad = 0; leftpad <= storage-size; leftpad++) {
                rightpad = (storage-size) - leftpad;

                SqlStringBuffer t(storage, size,
                                  leftpad, rightpad,
                                  'x', ' ', 
                                  leftbump, rightbump);

                bool caught = false;
                try {
                    SqlDateToStr<1,1,SQLTIME>(t.mStr, storage, ticks_per_hour + size*ticks_per_minute + storage*ticks_per_sec);
                } catch(const char *str) {
                    caught = true;
                    BOOST_CHECK_EQUAL(strcmp(str,"22001"),0);
                    BOOST_CHECK(t.verify());
                    BOOST_CHECK(storage < 10);
                } catch(...) {
                    BOOST_CHECK(false);
                }
                if (!caught) {
                    BOOST_CHECK(t.verify());
                    //              cout << t.mStr << endl;
                }

            }
        }
    }
}


// Helper to testSqlStrToDate_Ascii 
void
SqlDateTest::testSqlStrToDate_Ascii_Helper(SqlStrToDateConvAction action,
                                           uint64_t value,
                                           char const * const src,
                                           int len,
                                           bool errorExpected)

{
    int64_t t;
    bool caught = false;

    try {
        switch (action) {
        case StrToDate:
            t = IsoStringToDate(src, len);
            break;
        case StrToTime:
            t = IsoStringToTime(src, len);
            break;
        case StrToTimestamp:
            t = IsoStringToTimestamp(src, len);
            break;
        default:
            BOOST_CHECK(false);
        }
    } catch (const char *str) {
        caught = true;
        BOOST_CHECK_EQUAL(strcmp(str, "22007"), 0);
    } catch (...) {
        // unexpected exception
        BOOST_CHECK(false);
    }
    BOOST_CHECK_EQUAL(errorExpected, caught);
    if (!caught) {
        BOOST_CHECK_EQUAL(value, t);
    }
}



void
SqlDateTest::testSqlStrToDate_Ascii()
{
    int64_t oct2k = 972086400000LL; // GMT + 0; - in milliseconds
    testSqlStrToDate_Ascii_Helper(
        StrToDate, oct2k, "2000-10-21", 10, false);
    testSqlStrToDate_Ascii_Helper(
        StrToDate, oct2k, "  2000-10-21  ", 14, false);
    testSqlStrToDate_Ascii_Helper(
        StrToDate, oct2k, "junk", 4, true);
    testSqlStrToDate_Ascii_Helper(
        StrToDate, oct2k, "2000-23-23", 10, true);
    testSqlStrToDate_Ascii_Helper(
        StrToDate, oct2k, "2000-2-30", 10, true);

    int64_t fourteen21 = ( ticks_per_hour*14 + 
                           ticks_per_minute * 21 + ticks_per_sec * 1) /1000;
    testSqlStrToDate_Ascii_Helper(
        StrToTime, fourteen21, "14:21:01", 8, false);
    testSqlStrToDate_Ascii_Helper(
        StrToTime, fourteen21, "14:21:1", 7, false);
    testSqlStrToDate_Ascii_Helper(
        StrToTime, fourteen21, "  14:21:01  ", 12, false);
    // TODO: Fractional seconds not handled
    //testSqlStrToDate_Ascii_Helper(
    //    StrToTime, fourteen21 + 987, "  14:21:01.987  ", 12, false);
    testSqlStrToDate_Ascii_Helper(
        StrToTime, fourteen21, "12:61:01", 8, true);
    testSqlStrToDate_Ascii_Helper(
        StrToTime, fourteen21, "junk", 4, true);
    testSqlStrToDate_Ascii_Helper(
        StrToTime, fourteen21, "12:34", 5, true);
        
    int64_t ts = oct2k + fourteen21;
    testSqlStrToDate_Ascii_Helper(
        StrToTimestamp, ts, "2000-10-21 14:21:01", 19, false);
    testSqlStrToDate_Ascii_Helper(
        StrToTimestamp, ts, "2000-10-21 14:21:1", 18, false);
    // TODO: Fractional seconds not handled
    //testSqlStrToDate_Ascii_Helper(
    //    StrToTimestamp, ts + 323, "2000-10-21 14:21:01.323", 19, false);
    testSqlStrToDate_Ascii_Helper(
        StrToTimestamp, ts, "  2000-10-21 14:21:01  ", 23, false);
    testSqlStrToDate_Ascii_Helper(
        StrToTimestamp, ts, "2000-10-21 27:21:01", 19, true);
    testSqlStrToDate_Ascii_Helper(
        StrToTimestamp, ts, "2000-10-32 01:21:01", 19, true);
    testSqlStrToDate_Ascii_Helper(
        StrToTimestamp, ts, "junk", 4, true);
    testSqlStrToDate_Ascii_Helper(
        StrToTimestamp, ts, "2323-6-25", 9, true);
}


void
SqlDateTest::testLocalTime()
{

    int64_t t = CurrentTime();
    int64_t ts = CurrentTimestamp();

    cout << "Time = " << t << endl;
    cout << "Time = " << ts << endl;
}


FENNEL_UNIT_TEST_SUITE(SqlDateTest);

