/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
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
#include "fennel/calc/SqlDate.h"
#include "fennel/common/TraceSource.h"
#include "fennel/test/SqlStringBuffer.h"
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


const int MAXLEN = 8;   // Must not be less than 5. Best >=7.
const int MAXRANDOM = 5;
const int MAXCMPLEN = 8;  // Must not be less than 3.

class SqlDateTest : virtual public TestBase, public TraceSource
{
    void testSqlStringBuffer_Ascii();
    void testSqlStringBuffer_UCS2();

    void testSqlDateToStr_Ascii();
    void testSqlTimeToStr_Ascii();
    void appendCharsToUCS2LikeString(string& str,
                                     int number,
                                     char character);

#ifdef HAVE_ICU
    string UnicodeToPrintable(const UnicodeString &s);
#endif

    
public:
    explicit SqlDateTest()
        : TraceSource(this,"SqlDateTest")
    {
        srand(time(NULL));
        FENNEL_UNIT_TEST_CASE(SqlDateTest, testSqlDateToStr_Ascii);
        FENNEL_UNIT_TEST_CASE(SqlDateTest, testSqlTimeToStr_Ascii);
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
    int storage, size, leftpad, rightpad;
    int leftbump = 2;
    int rightbump = 2;

    int64_t const ticks_per_day = boost::posix_time::ptime::time_rep_type::frac_sec_per_day();
    //    int64_t const ticks_per_hour = ticks_per_day/24;
    int64_t const ticks_per_month = ticks_per_day * 31LL;
    int64_t const ticks_per_year = ticks_per_day * 365LL;
    
    
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
                  cout << t.mStr << endl;
                }

            }
        }
    }
}

void
SqlDateTest::testSqlTimeToStr_Ascii()
{
    int storage, size, leftpad, rightpad;
    int leftbump = 2;
    int rightbump = 2;

    int64_t const ticks_per_day = boost::posix_time::ptime::time_rep_type::frac_sec_per_day();
    int64_t const ticks_per_hour = ticks_per_day/24;
    int64_t const ticks_per_minute = ticks_per_hour/60;
    int64_t const ticks_per_sec = ticks_per_minute/60;

    
    
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
    cout << s1.mStr << endl;
    

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
                  cout << t.mStr << endl;
                }

            }
        }
    }
}


FENNEL_UNIT_TEST_SUITE(SqlDateTest);

