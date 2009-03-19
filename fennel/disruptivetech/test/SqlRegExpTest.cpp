/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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
#include "fennel/disruptivetech/calc/SqlRegExp.h"
#include "fennel/common/TraceSource.h"
#include "fennel/disruptivetech/test/SqlStringBuffer.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <boost/regex.hpp>

#include <string>
#include <limits>
#include <iostream>

#ifdef HAVE_ICU
#include <unicode/unistr.h>
#include <unicode/uloc.h>
#endif

using namespace fennel;
using namespace std;

class SqlRegExpTest : virtual public TestBase, public TraceSource
{
    void testSqlRegExpLikeAsciiTrue();
    void testSqlRegExpLikeAsciiFalse();
    void testSqlRegExpLikeAsciiEscapeTrue();
    void testSqlRegExpLikeAsciiEscapeFalse();
    void testSqlRegExpLikeAsciiException();

    void testSqlRegExpSimilarAscii();
    void testSqlRegExpSimilarAsciiEscape();
    void testSqlRegExpSimilarAsciiException();

public:
    explicit SqlRegExpTest()
        : TraceSource(shared_from_this(),"SqlRegExpTest")
    {
        srand(time(NULL));

        FENNEL_UNIT_TEST_CASE(SqlRegExpTest, testSqlRegExpLikeAsciiTrue);
        FENNEL_UNIT_TEST_CASE(SqlRegExpTest, testSqlRegExpLikeAsciiFalse);
        FENNEL_UNIT_TEST_CASE(SqlRegExpTest, testSqlRegExpLikeAsciiEscapeTrue);
        FENNEL_UNIT_TEST_CASE(SqlRegExpTest, testSqlRegExpLikeAsciiEscapeFalse);
        FENNEL_UNIT_TEST_CASE(SqlRegExpTest, testSqlRegExpLikeAsciiException);

        FENNEL_UNIT_TEST_CASE(SqlRegExpTest, testSqlRegExpSimilarAscii);
        FENNEL_UNIT_TEST_CASE(SqlRegExpTest, testSqlRegExpSimilarAsciiEscape);
        FENNEL_UNIT_TEST_CASE(SqlRegExpTest, testSqlRegExpSimilarAsciiException);
    }

    virtual ~SqlRegExpTest()
    {
    }
};


void
SqlRegExpTest::testSqlRegExpLikeAsciiTrue()
{
    bool result = false;
    int i;

    const char* test[][2] = {
        // pattern, matchValue

        // SQL99 Part 2 Section 8.5 General Rule 3.d.i
        { "",    "" },

        { "_",   "a" },
        { "a",   "a" },
        { "abc", "abc" },
        { "_bc", "abc" },
        { "a_c", "abc" },
        { "ab_", "abc" },
        { "a__", "abc" },
        { "_b_", "abc" },
        { "__c", "abc" },

        { "%",   "" },
        { "%",   "a" },
        { "%",   "abc" },
        { "%b",  "ab" },
        { "a%",  "ab" },
        { "ab%", "abc" },
        { "a%c", "abc" },
        { "%bc", "abc" },
        { "a%",  "abc" },
        { "%b%", "abc" },
        { "%c",  "abc" },
        { "%abc","abc" },
        { "abc%","abc" },

        // ensure that regex special chars are OK
        // and escaped properly
        { ".|*?+(){}[]^$\\",  ".|*?+(){}[]^$\\" },
        { "%.|*?+(){}[]^$\\", ".|*?+(){}[]^$\\" },
        { ".|*?+(){}[]^$\\%", ".|*?+(){}[]^$\\" },
        { "%){}[]^$\\",       ".|*?+(){}[]^$\\" },
        { ".|*?+()%",         ".|*?+(){}[]^$" },
        { "%$",               ".|*?+(){}[]^$" },
        { ".|*%",             ".|*?+(){}[]^$" },

        { "\\",      "\\" },
        { "a\\c",    "a\\c"},
        { "a\\%de",  "a\\cde"},
        { "a\\_de",  "a\\cde"},
        { "a\\.de",  "a\\.de"},

        { "X", "X" }  // end sentinal

    };
    string expPat;
    for (i = 0; *test[i][0] != 'X'; i++) {
        BOOST_MESSAGE("      true " << i << " " <<test[i][0]);
        try {
            SqlLikePrep<1,1>(test[i][0],
                             strlen(test[i][0]),
                             0, 0,    // no escape
                             expPat);
            boost::regex exp(expPat);
            result = SqlRegExp<1,1>(test[i][1],
                                    strlen(test[i][1]),
                                    strlen(test[i][0]),
                                    exp);

        }
        catch (char const * const ptr) {
            // unexpected exception
            BOOST_MESSAGE("unexpected SQL exception: " << ptr);
            BOOST_CHECK(0);
        }
        catch (boost::bad_expression badexp) {
            // regex format problem
            BOOST_MESSAGE("unexpected regex exception: "
                          <<badexp.what());
            BOOST_CHECK(0);

        }
        catch (...) {
            // unexpected exception
            BOOST_MESSAGE("unexpected unknown exception");
            BOOST_CHECK(0);
        }

        if (!result) {
            BOOST_MESSAGE("|" << test[i][1] <<
                          "| |" << test[i][0] << "|");
        }

        BOOST_CHECK(result);
    }
}

void
SqlRegExpTest::testSqlRegExpLikeAsciiFalse()
{
    bool result = false;
    int i;
    const char* test[][2] = {
        // pattern,    matchValue
        { "",    "a" },
        { "_",    "" },
        { "a",    "" },
        { "a",    "b" },
        { "b",    "ab" },
        { "a",    "ab" },
        { "__",    "a" },
        { "abc",    "Abc" },
        { "abc",    "aBc" },
        { "abc",    "abC" },
        { "_bc",    "aBc" },
        { "_bc",    "abC" },
        { "a_c",    "Abc" },
        { "ab_",    "aBc" },
        { "a__",    "Abc" },
        { "_b_",    "aBc" },
        { "__c",    "abC" },

        { "%b",     "a" },
        { "a%",     "b" },
        { "ab%",    "ac" },
        { "a%c",    "ab" },
        { "%bc",    "ab" },
        { "%b%",    "aBc" },
        { "%c",     "ab" },
        { "%abc","ac" },
        { "%abc","bc" },
        { "%abc","ab" },
        { "abc%","ab" },
        { "abc%","ac" },
        { "abc%","bc" },

        { "\\",      "a" },
        { "a",       "\\" },
        { "a\\c",    "a\\"},
        { "a\\c",    "\\c"},
        { "\\c",    "a\\"},
        { "a\\",    "\\c"},

        { "X",    "X" }  // end sentinal

    };
    string expPat;
    for (i = 0; *test[i][0] != 'X'; i++) {
        BOOST_MESSAGE("      false " << i << " " <<test[i][0]);
        try {
            SqlLikePrep<1,1>(test[i][0],
                             strlen(test[i][0]),
                             0, 0,   // no escape
                             expPat);
            boost::regex exp(expPat);
            result = SqlRegExp<1,1>(test[i][1],
                                    strlen(test[i][1]),
                                    strlen(test[i][0]),
                                    exp);
        }
        catch (char const * const ptr) {
            // unexpected exception
            BOOST_MESSAGE("unexpected SQL exception: " << ptr);
            BOOST_CHECK(0);
        }
        catch (boost::bad_expression badexp) {
            // regex format problem
            BOOST_MESSAGE("unexpected regex exception: "
                          <<badexp.what());
            BOOST_CHECK(0);

        }
        catch (...) {
            // unexpected exception
            BOOST_MESSAGE("unexpected unknown exception");
            BOOST_CHECK(0);
        }


        if (result) {
            BOOST_MESSAGE("|" << test[i][1] <<
                          "| |" << test[i][0] << "|");
        }

        BOOST_CHECK(!result);
    }
}


void
SqlRegExpTest::testSqlRegExpLikeAsciiEscapeTrue()
{
    bool result = false;
    int i;

    const char* test[][3] = {
        // pattern, matchValue, escape
        // define new escape
        { "_",      "a",  "#" },
        { "#_",    "_",   "#" },
        { "##",    "#",   "#" },
        { "#_bc",  "_bc", "#" },
        { "a#_c",  "a_c", "#" },
        { "ab#_",  "ab_", "#" },

        { "#%",    "%",   "#" },
        { "#%bc",  "%bc", "#" },
        { "a#%c",  "a%c", "#" },
        { "ab#%",  "ab%", "#" },
        { "%",     "a",   "#" },
        { "#%",    "%",   "#" },

        // define new escape that is special regexp char
        { "_",     "a",   "|" },
        { "|_",    "_",   "|" },
        { "||",    "|",   "|" },
        { "|_bc",  "_bc", "|" },
        { "a|_c",  "a_c", "|" },
        { "ab|_",  "ab_", "|" },

        { "|%",    "%",   "|" },
        { "|%bc",  "%bc", "|" },
        { "a|%c",  "a%c", "|" },
        { "ab|%",  "ab%", "|" },
        { "%",     "a",   "|" },
        { "|%",    "%",   "|" },

        // define new escape that is special regexp char
        { "_",     "a",   ")" },
        { ")_",    "_",   ")" },
        { "))",    ")",   ")" },
        { ")_bc",  "_bc", ")" },
        { "a)_c",  "a_c", ")" },
        { "ab)_",  "ab_", ")" },

        { ")%",    "%",   ")" },
        { ")%bc",  "%bc", ")" },
        { "a)%c",  "a%c", ")" },
        { "ab)%",  "ab%", ")" },
        { "%",     "a",   ")" },
        { ")%",    "%",   ")" },

        { "X",     "X",   "X" }  // end sentinal

    };
    string expPat;
    for (i = 0; *test[i][0] != 'X'; i++) {
        BOOST_MESSAGE("      escape true " << i << " " <<test[i][0]);
        try {
            SqlLikePrep<1,1>(test[i][0],
                             strlen(test[i][0]),
                             test[i][2],
                             strlen(test[i][2]),
                             expPat);
            boost::regex exp(expPat);
            result = SqlRegExp<1,1>(test[i][1],
                                    strlen(test[i][1]),
                                    strlen(test[i][0]),
                                    exp);
        }
        catch (char const * const ptr) {
            // unexpected exception
            BOOST_MESSAGE("unexpected SQL exception: " << ptr);
            BOOST_CHECK(0);
        }
        catch (boost::bad_expression badexp) {
            // regex format problem
            BOOST_MESSAGE("unexpected regex exception: "
                          <<badexp.what());
            BOOST_CHECK(0);

        }
        catch (...) {
            // unexpected exception
            BOOST_MESSAGE("unexpected unknown exception");
            BOOST_CHECK(0);
        }

        if (!result) {
            BOOST_MESSAGE("|" << test[i][1] <<
                          "| |" << test[i][0] << "|");
        }

        BOOST_CHECK(result);
    }
}

void
SqlRegExpTest::testSqlRegExpLikeAsciiEscapeFalse()
{
    bool result = false;
    int i;

    const char* test[][3] = {
        // pattern, matchValue, escape

        { "_",    "ab",  "#" },
        { "#_",   "_a",  "#" },
        { "#_",   "a_",  "#" },
        { "#_",   "a",   "#" },
        { "#_",   "__",  "#" },
        { "#_",   "a",   "#" },
        { "##",   "a",   "#" },
        { "#_#_", "a",   "#" },
        { "#_#_", "_",   "#" },
        { "#_#_", "_a",  "#" },
        { "#_#_", "a_",  "#" },
        { "#_bc", "abc", "#" },
        { "a#_c", "abc", "#" },
        { "ab#_", "abc", "#" },

        { "#%",   "a",   "#" },
        { "#%",   "ab",  "#" },
        { "#%",   "a",   "#" },
        { "#%bc", "abc", "#" },
        { "a#%c", "abc", "#" },
        { "ab#%", "abc", "#" },

        // define escape that is special regexp char
        { "_",    "ab",  "|" },
        { "|_",   "_a",  "|" },
        { "|_",   "a_",  "|" },
        { "|_",   "a",   "|" },
        { "||",   "a",   "|" },
        { "|%",   "a",   "|" },
        { "|%",   "ab",  "|" },

        // define escape that is special regexp char
        { "_",    "ab",  ")" },
        { ")_",   "_a",  ")" },
        { ")_",   "a_",  ")" },
        { ")_",   "a",   ")" },
        { "))",   "a",   ")" },
        { ")%",   "a",   ")" },
        { ")%",   "ab",  ")" },


        { "X",    "X",   "X" }  // end sentinal

    };
    string expPat;
    for (i = 0; *test[i][0] != 'X'; i++) {
        BOOST_MESSAGE("      escapefalse " << i << " " <<test[i][0]);
        try {
            SqlLikePrep<1,1>(test[i][0],
                             strlen(test[i][0]),
                             test[i][2],
                             strlen(test[i][2]),
                             expPat);
            boost::regex exp(expPat);
            result = SqlRegExp<1,1>(test[i][1],
                                    strlen(test[i][1]),
                                    strlen(test[i][0]),
                                    exp);
        }
        catch (char const * const ptr) {
            // unexpected exception
            BOOST_MESSAGE("unexpected SQL exception: " << ptr);
            BOOST_CHECK(0);
        }
        catch (boost::bad_expression badexp) {
            // regex format problem
            BOOST_MESSAGE("unexpected regex exception: "
                          <<badexp.what());
            BOOST_CHECK(0);

        }
        catch (...) {
            // unexpected exception
            BOOST_MESSAGE("unexpected unknown exception");
            BOOST_CHECK(0);
        }

        if (result) {
            BOOST_MESSAGE("|" << test[i][1] <<
                          "| |" << test[i][0] << "|");
        }

        BOOST_CHECK(!result);
    }
}

void
SqlRegExpTest::testSqlRegExpLikeAsciiException()
{
    bool caught = false;
    bool result = false;
    int i;

    const char* test[][4] = {
        // pattern, matchValue, escape, exception
        { "=",       "a",       "=",       "22025" },
        { "=a",       "a",       "=",      "22025" },

        { "a",       "a",       "ab",      "22019" },
        { "a",       "a",       "\\\\",    "22019" },

        { "X",       "X",       "X",       "X" }  // end sentinal

    };
    string expPat;
    for (i = 0; *test[i][0] != 'X'; i++) {
        BOOST_MESSAGE("      exception " << i << " " <<test[i][0]);
        caught = false;
        try {
            SqlLikePrep<1,1>(test[i][0],
                             strlen(test[i][0]),
                             test[i][2],
                             strlen(test[i][2]),
                             expPat);
            boost::regex exp(expPat);
            result = SqlRegExp<1,1>(test[i][1],
                                    strlen(test[i][1]),
                                    strlen(test[i][0]),
                                    exp);
        }
        catch (char const * const ex) {
            caught = true;
            BOOST_CHECK(!strcmp(ex,       test[i][3]));
        }
        if (!caught) {
            BOOST_CHECK(0);
        }
    }
}

void
SqlRegExpTest::testSqlRegExpSimilarAscii()
{
    bool result = false;
    int i;

    const char* test[][3] = {
        // pattern, matchValue, result

        // {2}
        // SQL2003 Part 2 Section 8.6 General Rule 6.a & 7.d
        { "a{2}",      "aa",     "t" },
        { "a{2}b",     "aab",    "t" },
        { "(bc){2}",   "bcbc",   "t" },
        { "(bc){2}d",  "bcbcd",  "t" },
        { "a(bc){2}",  "abcbc",  "t" },
        { "[bc]{2}",   "bb",     "t" },
        { "[bc]{2}",   "cc",     "t" },
        { "[bc]{2}",   "bc",     "t" },
        { "[bc]{2}",   "cb",     "t" },

        { "a{2}",      "",       "f" },
        { "a{2}",      "a",      "f" },
        { "a{2}",      "ac",     "f" },
        { "a{2}",      "aaa",    "f" },
        { "a{2}",      "ab",     "f" },
        { "a{2}b",     "",       "f" },
        { "a{2}b",     "b",      "f" },
        { "a{2}b",     "ab",     "f" },
        { "a{2}b",     "aa",     "f" },
        { "a{2}b",     "ab",     "f" },
        { "(bc){2}",   "",       "f" },
        { "(bc){2}",   "a",      "f" },
        { "(bc){2}",   "bc",     "f" },
        { "(bc){2}",   "bcbcb",  "f" },
        { "[bc]{2}",   "",       "f" },
        { "[bc]{2}",   "a",      "f" },
        { "[bc]{2}",   "b",      "f" },
        { "[bc]{2}",   "c",      "f" },
        { "[bc]{2}",   "ad",     "f" },
        { "[bc]{2}",   "",       "f" },
        { "[bc]{2}",   "bbc",    "f" },

        // {2,3}
        // SQL2003 Part 2 Section 8.6 General Rule 6.b & 7.d
        // <upper limit> w/ <high value>
        { "a{2,3}",    "aa",     "t" },
        { "a{2,3}",    "aaa",    "t" },
        { "a{2,3}b",   "aab",    "t" },
        { "a{2,3}b",   "aaab",   "t" },
        { "(bc){2,3}", "bcbc",   "t" },
        { "(bc){2,3}", "bcbcbc", "t" },
        { "[bc]{2,3}", "bb",     "t" },
        { "[bc]{2,3}", "bbb",    "t" },
        { "[bc]{2,3}", "cc",     "t" },
        { "[bc]{2,3}", "ccc",    "t" },
        { "[bc]{2,3}", "bcb",    "t" },
        { "[bc]{2,3}", "cbc",    "t" },

        { "a{2,3}",    "",       "f" },
        { "a{2,3}",    "a",      "f" },
        { "a{2,3}",    "aaaa",   "f" },
        { "a{2,3}",    "aab",    "f" },
        { "(bc){2,3}", "",       "f" },
        { "(bc){2,3}", "a",      "f" },
        { "(bc){2,3}", "bc",     "f" },
        { "(bc){2,3}", "cbcb",   "f" },
        { "[bc]{2,3}", "",       "f" },
        { "[bc]{2,3}", "a",      "f" },
        { "[bc]{2,3}", "b",      "f" },
        { "[bc]{2,3}", "c",      "f" },
        { "[bc]{2,3}", "bcbcbc", "f" },
        { "[bc]{2,3}", "bcbcbcb","f" },
        { "[bc]{2,3}", "",       "f" },
        { "[bc]{2,3}", "bbcc",   "f" },

        // {2,}
        // SQL2003 Part 2 Section 8.6 General Rule 6.c & 7.d
        // <upper limit> w/o <high value>
        // 98.6% sure that I'm interpreting this correctly. -JK 2004/6
        { "a{2,}",    "aa",     "t" },
        { "a{2,}",    "aaa",    "t" },
        { "a{2,}",    "aaaa",    "t" },
        { "a{2,}b",   "aab",    "t" },
        { "a{2,}b",   "aaab",   "t" },
        { "(bc){2,}", "bcbc",   "t" },
        { "(bc){2,}", "bcbcbc", "t" },
        { "[bc]{2,}", "bb",     "t" },
        { "[bc]{2,}", "bbb",    "t" },
        { "[bc]{2,}", "cc",     "t" },
        { "[bc]{2,}", "ccc",    "t" },
        { "[bc]{2,}", "bcb",    "t" },
        { "[bc]{2,}", "cbc",    "t" },

        { "a{2,}",    "",       "f" },
        { "a{2,}",    "a",      "f" },
        { "a{2,}",    "aab",    "f" },
        { "(bc){2,}", "",       "f" },
        { "(bc){2,}", "a",      "f" },
        { "(bc){2,}", "bc",     "f" },
        { "(bc){2,}", "cbcb",   "f" },
        { "(bc){2,}", "bcbcb",  "f" },
        { "[bc]{2,}", "",       "f" },
        { "[bc]{2,}", "a",      "f" },
        { "[bc]{2,}", "b",      "f" },
        { "[bc]{2,}", "c",      "f" },
        { "[bc]{2,}", "bcd",    "f" },

        // |
        // SQL2003 Part 2 Section 8.6 General Rule 7.a
        { "a|b",      "a",      "t" },
        { "a|b",      "b",      "t" },
        { "a|bc",     "a",      "t" },
        { "a|bc",     "bc",     "t" },
        { "(a|b)c",   "ac",     "t" },
        { "(a|b)c",   "bc",     "t" },

        { "a|b",      "c",      "f" },
        { "a|bc",     "c",      "f" },
        { "a|bc",     "ac",     "f" },
        { "(a|b)c",   "c",      "f" },
        { "(a|b)c",   "dc",     "f" },

        // *
        // SQL2003 Part 2 Section 8.6 General Rule 7.b
        { "a*b",      "b",      "t" },
        { "a*b",      "ab",     "t" },
        { "a*b",      "aab",    "t" },
        { "ab*",      "a",      "t" },
        { "ab*",      "ab",     "t" },
        { "ab*",      "abb",    "t" },
        { "a(bc)*",   "a",      "t" },
        { "a(bc)*",   "abc",    "t" },
        { "a(bc)*",   "abcbc",  "t" },
        { "a[bc]*",   "a",      "t" },
        { "a[bc]*",   "ab",     "t" },
        { "a[bc]*",   "ac",     "t" },
        { "a[bc]*",   "abb",    "t" },
        { "a[bc]*",   "abc",    "t" },
        { "a[bc]*",   "abc",    "t" },
        { "a[bc]*",   "acc",    "t" },
        { "a[bc]*",   "abbb",   "t" },
        { "a[bc]*",   "accc",   "t" },

        { "a*b",      "",       "f" },
        { "a*b",      "a",      "f" },
        { "a*b",      "ac",     "f" },
        { "ab*",      "b" ,     "f" },
        { "ab*",      "ac",     "f" },
        { "a(bc)*",   "",       "f" },
        { "a(bc)*",   "ad",     "f" },
        { "a(bc)*",   "abd",    "f" },
        { "a(bc)*",   "adb",    "f" },
        { "a[bc]*",   "",       "f" },
        { "a[bc]*",   "ad",     "f" },
        { "a[bc]*",   "abd",    "f" },
        { "a[bc]*",   "acd",    "f" },

        // +
        // SQL2003 Part 2 Section 8.6 General Rule 7.c
        { "a+b",      "ab",     "t" },
        { "a+b",      "aab",    "t" },
        { "ab+",      "ab",     "t" },
        { "ab+",      "abb",    "t" },
        { "a(bc)+",   "abc",    "t" },
        { "a(bc)+",   "abcbc",  "t" },
        { "a[bc]+",   "ab",     "t" },
        { "a[bc]+",   "abb",    "t" },
        { "a[bc]+",   "ac",     "t" },
        { "a[bc]+",   "acc",    "t" },
        { "a[bc]+",   "abc",    "t" },
        { "a[bc]+",   "abccb",  "t" },

        { "a+b",      "",       "f" },
        { "a+b",      "a",      "f" },
        { "a+b",      "b",      "f" },
        { "a+b",      "ac",     "f" },
        { "ab+",      "",       "f" },
        { "ab+",      "a",      "f" },
        { "ab+",      "b" ,     "f" },
        { "ab+",      "ac",     "f" },
        { "a(bc)+",   "",       "f" },
        { "a(bc)+",   "a",      "f" },
        { "a(bc)+",   "ad",     "f" },
        { "a(bc)+",   "abd",    "f" },
        { "a(bc)+",   "adb",    "f" },
        { "a[bc]+",   "",       "f" },
        { "a[bc]+",   "a",      "f" },
        { "a[bc]+",   "ad",     "f" },
        { "a[bc]+",   "abd",    "f" },
        { "a[bc]+",   "acd",    "f" },

        // General Rule 7.d is above with GR6

        // SQL2003 Part 2 Section 8.6 General Rule 7.e
        { "a",         "a",     "t" },
        { "a",         "",      "f" },

        // %
        // SQL2003 Part 2 Section 8.6 General Rule 7.f
        { "%",        "",       "t" },
        { "%",        "a",      "t" },
        { "%",        "abc",    "t" },
        { "%b",       "ab",     "t" },
        { "a%",       "ab",     "t" },
        { "ab%",      "abc",    "t" },
        { "a%c",      "abc",    "t" },
        { "%bc",      "abc",    "t" },
        { "a%",       "abc",    "t" },
        { "%b%",      "abc",    "t" },
        { "%c",       "abc",    "t" },
        { "%abc",     "abc",    "t" },
        { "abc%",     "abc",    "t" },

        { "%b",       "a",      "f" },
        { "a%",       "b",      "f" },
        { "ab%",      "ac",     "f" },
        { "a%c",      "ab",     "f" },
        { "%bc",      "ab",     "f" },
        { "%b%",      "aBc",    "f" },
        { "%c",       "ab",     "f" },
        { "%abc",     "ac",     "f" },
        { "%abc",     "bc",     "f" },
        { "%abc",     "ab",     "f" },
        { "abc%",     "ab",     "f" },
        { "abc%",     "ac",     "f" },
        { "abc%",     "bc",     "f" },

        // ?
        // SQL2003 Part 2 Section 8.6 General Rule 7.g
        { "a?b",      "b",      "t" },
        { "a?b",      "ab",     "t" },
        { "ab?",      "a",      "t" },
        { "ab?",      "ab",     "t" },
        { "a(bc)?",   "a",      "t" },
        { "a(bc)?",   "abc",    "t" },
        { "a[bc]?",   "a",      "t" },
        { "a[bc]?",   "ab",     "t" },
        { "a[bc]?",   "ac",     "t" },

        { "a?b",      "",       "f" },
        { "a?b",      "a",      "f" },
        { "a?b",      "ac",     "f" },
        { "a?b",      "aab",    "f" },
        { "ab?",      "abb",    "f" },
        { "ab?",      "b" ,     "f" },
        { "ab?",      "ac",     "f" },
        { "a(bc)?",   "",       "f" },
        { "a(bc)?",   "abcbc",  "f" },
        { "a(bc)?",   "ad",     "f" },
        { "a(bc)?",   "abd",    "f" },
        { "a(bc)?",   "adb",    "f" },
        { "a[bc]?",   "",       "f" },
        { "a[bc]?",   "ad",     "f" },
        { "a[bc]?",   "abd",    "f" },
        { "a[bc]?",   "acd",    "f" },
        { "a[bc]?",   "abb",    "f" },
        { "a[bc]?",   "abc",    "f" },
        { "a[bc]?",   "abc",    "f" },
        { "a[bc]?",   "acc",    "f" },
        { "a[bc]?",   "abbb",   "f" },
        { "a[bc]?",   "accc",   "f" },

        // SQL2003 Part 2 Section 8.6 General Rule 7.h
        // also mixed with other tests
        { "(a)",      "a",      "t" },
        { "(ab)",     "ab",     "t" },
        { "(a)(b)",   "ab",     "t" },
        { "a(b)(c)d", "abcd",   "t" },
        { "(a(b))",   "ab",     "t" },

        { "(a)",      "",       "f" },
        { "(a)",      "b",      "f" },
        { "(ab)",     "a",      "f" },
        { "(ab)",     "b",      "f" },
        { "(a)(b)",   "a",      "f" },
        { "(a)(b)",   "b",      "f" },
        { "(a)(b)",   "abc",    "f" },
        { "a(b)(c)d", "abc",    "f" },
        { "a(b)(c)d", "bcd",    "f" },
        { "(a(b))",   "abc",    "f" },

        // _
        // SQL2003 Part 2 Section 8.6 General Rule 7.i
        { "_",        "a",      "t" },
        { "a",        "a",      "t" },
        { "abc",      "abc",    "t" },
        { "_bc",      "abc",    "t" },
        { "a_c",      "abc",    "t" },
        { "ab_",      "abc",    "t" },
        { "a__",      "abc",    "t" },
        { "_b_",      "abc",    "t" },
        { "__c",      "abc",    "t" },

        { "_",        "",       "f" },
        { "a",        "",       "f" },
        { "a",        "b",      "f" },
        { "b",        "ab",     "f" },
        { "a",        "ab",     "f" },
        { "__",       "a",      "f" },
        { "abc",      "Abc",    "f" },
        { "abc",      "aBc",    "f" },
        { "abc",      "abC",    "f" },
        { "_bc",      "aBc",    "f" },
        { "_bc",      "abC",    "f" },
        { "a_c",      "Abc",    "f" },
        { "ab_",      "aBc",    "f" },
        { "a__",      "Abc",    "f" },
        { "_b_",      "aBc",    "f" },
        { "__c",      "abC",    "f" },

        // [a], [ab], [a-c]
        // SQL2003 Part 2 Section 8.6 General Rule 7.j
        // SQL2003 Part 2 Section 8.6 General Rule 5.a & 5.b
        // (General Rule 5b is tested throughout below)
        { "[a]",       "a",     "t" },
        { "[ab]",      "a",     "t" },
        { "[ab]",      "b",     "t" },
        { "[a-c]",     "a",     "t" },
        { "[a-c]",     "b",     "t" },
        { "[a-c]",     "c",     "t" },

        { "[a]",       "",      "f" },
        { "[a]",       "b",     "f" },
        { "[a]",       "ab",    "f" },
        { "[ab]",      "",      "f" },
        { "[ab]",      "c",     "f" },
        { "[ab]",      "ab",    "f" },
        { "[a-c]",     "",      "f" },
        { "[a-c]",     "Z",     "f" },
        { "[a-c]",     "d",     "f" },

        // [^a], [^ab], [^a-c]
        // SQL2003 Part 2 Section 8.6 General Rule 7.k
        // SQL2003 Part 2 Section 8.6 General Rule 5.a & 5.b
        { "[^a]",      "b",     "t" },
        { "[^ab]",     "c",     "t" },
        { "[^a-c]",    "d",     "t" },

        { "[^a]",      "",      "f" },
        { "[^a]",      "a",     "f" },
        { "[^a]",      "ab",    "f" },
        { "[^ab]",     "",      "f" },
        { "[^ab]",     "a",     "f" },
        { "[^ab]",     "b",     "f" },
        { "[^ab]",     "ab",    "f" },
        { "[^a-c]",    "",      "f" },
        { "[^a-c]",    "a",     "f" },
        { "[^a-c]",    "b",     "f" },
        { "[^a-c]",    "c",     "f" },
        { "[^a-c]",    "ab",    "f" },

        // [a^b], [a-c^d-f]
        // SQL2003 Part 2 Section 8.6 General Rule 7.l (7L)
        // boost regex does not support this
        // SqlSimilarPrep does not currently have a workaround.
        // TODO: Add a workaround in SqlSimilarPrep to allow this to work
#if 0
        { "[a^b]",     "ac",    "t" },
        { "[a^b]",     "aa",    "t" },
        { "[a-c^d-f]", "ad",    "t" },
        { "[a-c^d-f]", "cf",    "t" },

        { "[a^b]",     "",      "f" },
        { "[a^b]",     "a",     "f" },
        { "[a^b]",     "b",     "f" },
        { "[a^b]",     "bb",    "f" },
        { "[a^b]",     "ab",    "f" },
        { "[a^b]",     "acd",   "f" },
        { "[a-c^d-f]", "",      "f" },
        { "[a-c^d-f]", "aa",    "f" },
        { "[a-c^d-f]", "ag",    "f" },
        { "[a-c^d-f]", "ca",    "f" },
        { "[a-c^d-f]", "cg",    "f" },
        { "[a-c^d-f]", "ad",    "f" },
        { "[a-c^d-f]", "af",    "f" },
        { "[a-c^d-f]", "aaa",   "f" },
#endif

        // SQL2003 Part 2 Section 8.6 General Rule 7.m
        { "[[:alpha:]]",  "a",  "t" },
        { "[[:ALPHA:]]",  "a",  "t" },
        { "[[:ALPHA:]]",  "A",  "t" },
        { "[^[:alpha:]]", "1",  "t" },
        { "[^[:ALPHA:]]", "1",  "t" },

        { "[[:ALPHA:]]",  "",   "f" },
        { "[[:ALPHA:]]",  " ",  "f" },
        { "[[:ALPHA:]]",  "\t", "f" },
        { "[[:ALPHA:]]",  "\n", "f" },
        { "[[:ALPHA:]]",  "1",  "f" },
        { "[[:ALPHA:]]",  "@",  "f" },
        { "[[:ALPHA:]]",  "a1", "f" },
        { "[[:ALPHA:]]",  "aa", "f" },
        { "[^[:ALPHA:]]", "a",  "f" },
        { "[^[:ALPHA:]]", "A",  "f" },

        // SQL2003 Part 2 Section 8.6 General Rule 7.n
        { "[[:upper:]]",  "A",  "t" },
        { "[[:UPPER:]]",  "A",  "t" },
        { "[^[:upper:]]", "1",  "t" },
        { "[^[:UPPER:]]", "1",  "t" },
        { "[^[:UPPER:]]", "a",  "t" },

        { "[[:UPPER:]]",  "",   "f" },
        { "[[:UPPER:]]",  " ",  "f" },
        { "[[:UPPER:]]",  "\t", "f" },
        { "[[:UPPER:]]",  "\n", "f" },
        { "[[:UPPER:]]",  "1",  "f" },
        { "[[:UPPER:]]",  "@",  "f" },
        { "[[:UPPER:]]",  "a",  "f" },
        { "[[:UPPER:]]",  "AA", "f" },
        { "[^[:UPPER:]]", "A",  "f" },

        // SQL2003 Part 2 Section 8.6 General Rule 7.o
        { "[[:lower:]]",  "a",  "t" },
        { "[[:LOWER:]]",  "a",  "t" },
        { "[^[:lower:]]", "1",  "t" },
        { "[^[:LOWER:]]", "1",  "t" },
        { "[^[:LOWER:]]", "A",  "t" },

        { "[[:LOWER:]]",  "",   "f" },
        { "[[:LOWER:]]",  " ",  "f" },
        { "[[:LOWER:]]",  "\t", "f" },
        { "[[:LOWER:]]",  "\n", "f" },
        { "[[:LOWER:]]",  "1",  "f" },
        { "[[:LOWER:]]",  "@",  "f" },
        { "[[:LOWER:]]",  "A",  "f" },
        { "[[:LOWER:]]",  "aa", "f" },
        { "[^[:LOWER:]]", "a",  "f" },

        // SQL2003 Part 2 Section 8.6 General Rule 7.p
        { "[[:digit:]]",  "1",  "t" },
        { "[[:DIGIT:]]",  "1",  "t" },
        { "[^[:digit:]]", "a",  "t" },
        { "[^[:DIGIT:]]", "a",  "t" },
        { "[^[:DIGIT:]]", "a",  "t" },

        { "[[:DIGIT:]]",  "",   "f" },
        { "[[:DIGIT:]]",  " ",  "f" },
        { "[[:DIGIT:]]",  "\t", "f" },
        { "[[:DIGIT:]]",  "\n", "f" },
        { "[[:DIGIT:]]",  "a",  "f" },
        { "[[:DIGIT:]]",  "@",  "f" },

        // SQL2003 Part 2 Section 8.6 General Rule 7.q
        { "[[:space:]]",  " ",  "t" },
        { "[[:SPACE:]]",  " ",  "t" },
        { "[^[:space:]]", "a",  "t" },
        { "[^[:SPACE:]]", "a",  "t" },
        { "[^[:SPACE:]]", "\t", "t" },
        { "[^[:SPACE:]]", "\n", "t" },

        { "[[:SPACE:]]",  "",   "f" },
        { "[^[:SPACE:]]", " ",  "f" },
        { "[^[:SPACE:]]", "  ", "f" },
        { "[[:SPACE:]]",  "\t", "f" },
        { "[[:SPACE:]]",  "\n", "f" },
        { "[[:SPACE:]]",  "a",  "f" },
        { "[[:SPACE:]]",  "@",  "f" },


        // SQL2003 Part 2 Section 8.6 General Rule 7.r
        { "[[:whitespace:]]",  " ",  "t" },
        { "[[:WHITESPACE:]]",  " ",  "t" },
        { "[[:WHITESPACE:]]",  "\t",  "t" },
        { "[[:WHITESPACE:]]",  "\n",  "t" },
        { "[[:WHITESPACE:]]",  "\v",  "t" },
        { "[[:WHITESPACE:]]",  "\f",  "t" },
        { "[[:WHITESPACE:]]",  "\r",  "t" },
        { "[[:WHITESPACE:]]",  "\x20",  "t" },
        { "[[:WHITESPACE:]]",  "\xa0",  "t" },
        { "[[:WHITESPACE:]]",  "\x09",  "t" },
        { "[[:WHITESPACE:]]",  "\x0a",  "t" },
        { "[[:WHITESPACE:]]",  "\x0b",  "t" },
        { "[[:WHITESPACE:]]",  "\x0c",  "t" },
        { "[[:WHITESPACE:]]",  "\x0d",  "t" },
        { "[[:WHITESPACE:]]",  "\x85",  "t" },
        { "[^[:whitespace:]]", "a",  "t" },
        { "[^[:WHITESPACE:]]", "a",  "t" },

        { "[^[:WHITESPACE:]]", "\t", "f" },
        { "[^[:WHITESPACE:]]", "\n", "f" },
        { "[[:WHITESPACE:]]",  "",   "f" },
        { "[^[:WHITESPACE:]]", " ",  "f" },
        { "[^[:WHITESPACE:]]", "  ", "f" },
        { "[[:WHITESPACE:]]",  "a",  "f" },
        { "[[:WHITESPACE:]]",  "@",  "f" },

        // SQL2003 Part 2 Section 8.6 General Rule 7.s
        { "[[:alnum:]]",  "a",  "t" },
        { "[[:ALNUM:]]",  "a",  "t" },
        { "[[:ALNUM:]]",  "1",  "t" },
        { "[[:ALNUM:]]",  "A",  "t" },
        { "[^[:alnum:]]", "!",  "t" },
        { "[^[:ALNUM:]]", "!",  "t" },
        { "[^[:ALNUM:]]", " ",  "t" },
        { "[^[:ALNUM:]]", "\t", "t" },
        { "[^[:ALNUM:]]", "\n", "t" },

        { "[[:ALNUM:]]",  "",   "f" },
        { "[[:ALNUM:]]",  " ",  "f" },
        { "[[:ALNUM:]]",  "\t", "f" },
        { "[[:ALNUM:]]",  "\n", "f" },
        { "[^[:ALNUM:]]", "1",  "f" },
        { "[^[:ALNUM:]]", "a",  "f" },
        { "[^[:ALNUM:]]", "A",  "f" },
        { "[[:ALNUM:]]",  "aa", "f" },

        // SQL2003 Part 2 Section 8.6 General Rule 7.t
        // TODO: Understand and implement 7.t. (Confused.)
#if 0
        { "||",       "a",      "t" },
        { "||",       "aa",     "f" },
#endif

        // SQL2003 Part 2 Section 8.6 General Rule 7.u
        { "",         "",       "t" },
        { "",         "a",      "f" },


        // search for characters special to regex, but not to SQL
        { "\\",       "\\",     "t" },
        { "\\",       "a",      "f" },
        { "$",        "$",      "t" },
        { "a$c",      "a$c",    "t" },
        { "a$c",      "abc",    "f" },
        { ".",        ".",      "t" },
        { ".",        "a",      "f" },

        { "X",  "X",  "X" }  // end sentinal

    };
    string expPat;
    for (i = 0; *test[i][0] != 'X'; i++) {
        BOOST_MESSAGE(" ===== Ascii Similar " << i <<
                      " " << test[i][0] << " " <<
                      test[i][1] << " " << test[i][2]);
        try {
            SqlSimilarPrep<1,1>(test[i][0],
                                strlen(test[i][0]),
                                0, 0,   // no escape
                                expPat);

            boost::regex exp(expPat);
            result = SqlRegExp<1,1>(test[i][1],
                                    strlen(test[i][1]),
                                    strlen(test[i][0]),
                                    exp);

        }
        catch (char const * const ptr) {
            // unexpected exception
            BOOST_MESSAGE("unexpected SQL exception: " << ptr);
            BOOST_CHECK(0);
        }
        catch (boost::bad_expression badexp) {
            // regex format problem
            BOOST_MESSAGE("unexpected regex exception: "
                          <<badexp.what());
            BOOST_CHECK(0);

        }
        catch (...) {
            // unexpected exception
            BOOST_MESSAGE("unexpected unknown exception");
            BOOST_CHECK(0);
        }

        if (*(test[i][2]) == 't') {
            if (!result) {
                BOOST_MESSAGE("|" << test[i][1] <<
                              "| |" << test[i][0] << "| expPat=|" <<
                              expPat << "|");
            }
            BOOST_CHECK(result);
        } else {
            if (result) {
                BOOST_MESSAGE("|" << test[i][1] <<
                              "| |" << test[i][0] << "| expPat=|" <<
                              expPat << "|");
            }
            BOOST_CHECK(!result);
        }
    }
}

void
SqlRegExpTest::testSqlRegExpSimilarAsciiEscape()
{
    bool result = false;
    int i;

    const char* test[][4] = {
        // pattern, matchValue, escape

        // define a new escape
        { "_",       "a",       "#", "t" },
        { "#_",      "_",       "#", "t" },
        { "#_bc",    "_bc",     "#", "t" },
        { "a#_c",    "a_c",     "#", "t" },
        { "ab#_",    "ab_",     "#", "t" },
        { "#%",      "%",       "#", "t" },
        { "#%bc",    "%bc",     "#", "t" },
        { "a#%c",    "a%c",     "#", "t" },
        { "ab#%",    "ab%",     "#", "t" },
        { "%",       "a",       "#", "t" },
        { "#%",      "%",       "#", "t" },
        { "##",      "#",       "#", "t" },
        // try all special chars (both to SIMILAR & regex)
        { "#[#]#(#)#|#^#-#+#*#_#%#?#{#}$.\\",
          "[]()|^-+*_%?{}$.\\",  "#", "t" },
        { "#[#{#(#|#?#^#*#%#+#-#_#)#}#]$.\\",
          "[{(|?^*%+-_)}]$.\\",  "#", "t" },


        { "#%",      "a",       "#", "f" },
        { "##",      "a",       "#", "f" },
        { "#%",      "ab",      "#", "f" },
        { "#%",      "a",       "#", "f" },
        { "#%bc",    "abc",     "#", "f" },
        { "a#%c",    "abc",     "#", "f" },
        { "ab#%",    "abc",     "#", "f" },

        { "_",       "ab",      "#", "f" },
        { "#_",      "_a",      "#", "f" },
        { "#_",      "a_",      "#", "f" },
        { "#_",      "a",       "#", "f" },
        { "#_",      "__",      "#", "f" },
        { "#_",      "a",       "#", "f" },
        { "#_#_",    "a",       "#", "f" },
        { "#_#_",    "_",       "#", "f" },
        { "#_#_",    "_a",      "#", "f" },
        { "#_#_",    "a_",      "#", "f" },
        { "#_bc",    "abc",     "#", "f" },
        { "a#_c",    "abc",     "#", "f" },
        { "ab#_",    "abc",     "#", "f" },

        // define new escape that is special regexp char
        { "_",       "a",       "|", "t" },
        { "|_",      "_",       "|", "t" },
        { "|_bc",    "_bc",     "|", "t" },
        { "a|_c",    "a_c",     "|", "t" },
        { "ab|_",    "ab_",     "|", "t" },
        { "||",      "|",       "|", "t" },
        { "((",      "(",       "(", "t" },

        { "|%",      "%",       "|", "t" },
        { "|%bc",    "%bc",     "|", "t" },
        { "a|%c",    "a%c",     "|", "t" },
        { "ab|%",    "ab%",     "|", "t" },
        { "%",       "a",       "|", "t" },
        { "|%",      "%",       "|", "t" },

        // try a other special chars as escape
        { "[[[][([)[|[^[-[+[*[_[%[?[{[}$.\\",
          "[]()|^-+*_%?{}$.\\",  "[", "t" },
        { "[[[{[([|[?[^[*[%[+[-[_[)[}[]$.\\",
          "[{(|?^*%+-_)}]$.\\",  "[", "t" },
        { "][]]](])]|]^]-]+]*]_]%]?]{]}$.\\",
          "[]()|^-+*_%?{}$.\\",  "]", "t" },
        { ".[.].(.).|.^.-.+.*._.%.?.{.}$\\",
          "[]()|^-+*_%?{}$\\",  ".", "t" },
        { "*[*]*(*)*|*^*-*+***_*%*?*{*}$.\\",
          "[]()|^-+*_%?{}$.\\",  "*", "t" },
        { "_[_]_(_)_|_^_-_+_*___%_?_{_}$.\\",
          "[]()|^-+*_%?{}$.\\",  "_", "t" },
        { "%[%]%(%)%|%^%-%+%*%_%%%?%{%}$.\\",
          "[]()|^-+*_%?{}$.\\",  "%", "t" },

        { "_",       "ab",      "|", "f" },
        { "|_",      "_a",      "|", "f" },
        { "|_",      "a_",      "|", "f" },
        { "|_",      "a",       "|", "f" },
        { "|%",      "a",       "|", "f" },
        { "|%",      "ab",      "|", "f" },
        { "||",      "a",       "|", "f" },
        { "((",      "a",       "(", "f" },

        { "X",       "X",       "X", "X" }  // end sentinal

    };
    string expPat;
    for (i = 0; *test[i][0] != 'X'; i++) {
        BOOST_MESSAGE(" ========== escape " << i << " " <<test[i][0] <<
                      test[i][1] << " " << test[i][2] << " "
                      << test[i][3]);
        try {
            SqlSimilarPrep<1,1>(test[i][0],
                                strlen(test[i][0]),
                                test[i][2],
                                strlen(test[i][2]),
                                expPat);
            boost::regex exp(expPat);
            result = SqlRegExp<1,1>(test[i][1],
                                    strlen(test[i][1]),
                                    strlen(test[i][0]),
                                    exp);
        }
        catch (char const * const ptr) {
            // unexpected exception
            BOOST_MESSAGE("unexpected SQL exception: " << ptr);
            BOOST_CHECK(0);
        }
        catch (boost::bad_expression badexp) {
            // regex format problem
            BOOST_MESSAGE("unexpected regex exception: "
                          <<badexp.what());
            BOOST_CHECK(0);

        }
        catch (...) {
            // unexpected exception
            BOOST_MESSAGE("unexpected unknown exception");
            BOOST_CHECK(0);
        }


        if (*(test[i][3]) == 't') {
            if (!result) {
                BOOST_MESSAGE("|" << test[i][1] <<
                              "| |" << test[i][0] << "| expPat=|" <<
                              expPat << "|");
            }
            BOOST_CHECK(result);
        } else {
            if (result) {
                BOOST_MESSAGE("|" << test[i][1] <<
                              "| |" << test[i][0] << "| expPat=|" <<
                              expPat << "|");
            }
            BOOST_CHECK(!result);
        }
    }
}

void
SqlRegExpTest::testSqlRegExpSimilarAsciiException()
{
    bool caught = false;
    bool result = false;
    int i;

    const char* test[][4] = {
        // pattern, matchValue, escape, exception
        { "[[:ALPHA:]]",
                  "a",   ":",     "2200B" },
        { "[[:alpha:]]",
                  "a",   ":",     "2200B" },

        { "a",    "a",   "ab",    "22019" },
        { "a",    "a",   "\\\\",  "22019" },

        // escape char at end of string
        { "=",    "a",   "=",     "2201B" },
        // escaping a non-special char
        { "=a",   "a",   "=",     "2201B" },
        // invalid regular character set name
        { "[[:foo:]]",
                  "a",   "=",     "2201B" },
        // mixed case regular character set name
        // (code allows lower case, outside of standard)
        { "[[:Alnum:]]",
                  "a",   "=",     "2201B" },
        // sql-only special character in character set
        { "[_]",  "a",   "=",     "2201B" },
        { "[a_]", "a",   "=",     "2201B" },
        { "[%]",  "a",   "=",     "2201B" },
        { "[a%]", "a",   "=",     "2201B" },
        // regex & sql special character in character set
        { "[[]",  "a",   "=",     "2201B" }, // also opening w/o close
        { "[]]",  "a",   "=",     "2201B" }, // also close w/o open
        { "[(]",  "a",   "=",     "2201B" },
        { "[)]",  "a",   "=",     "2201B" },
        { "[|]",  "a",   "=",     "2201B" },
        { "[^]",  "a",   "=",     "regex" }, // thrown by regex
        // TODO: ? Could make this work, but seems at vanishing point of
        // utility. BNF says this is not legal.
        //{ "[-]",  "a",   "=",     "2201B" }, // technically should be caught
        { "[+]",  "a",   "=",     "2201B" },
        { "[*]",  "a",   "=",     "2201B" },
        { "[_]",  "a",   "=",     "2201B" },
        { "[?]",  "a",   "=",     "2201B" },
        { "[{]",  "a",   "=",     "2201B" },
        { "[}]",  "a",   "=",     "2201B" },


        { "[a]",  "a",   "[",    "2200C" },
        { "[a]",  "a",   "]",    "2200C" },
        { "(a)",  "a",   "(",    "2200C" },
        { "(a)",  "a",   ")",    "2200C" },
        { "a|b",  "a",   "|",    "2200C" },
        { "[^a]", "a",   "^",    "2200C" },
        { "[a-b]","a",   "-",    "2200C" },
        { "(a)+", "a",   "+",    "2200C" },
        { "(a)*", "a",   "*",    "2200C" },
        { "a_",   "a",   "_",    "2200C" },
        { "a%",   "a",   "%",    "2200C" },

        { "X",    "X",   "X",     "X" }  // end sentinal
    };
    string expPat;
    for (i = 0; *test[i][0] != 'X'; i++) {
        BOOST_MESSAGE(" ===== exception " << i << " " <<test[i][0]);
        caught = false;
        try {
            SqlSimilarPrep<1,1>(test[i][0],
                                strlen(test[i][0]),
                                test[i][2],
                                strlen(test[i][2]),
                                expPat);
            boost::regex exp(expPat);
            result = SqlRegExp<1,1>(test[i][1],
                                    strlen(test[i][1]),
                                    strlen(test[i][0]),
                                    exp);
        }
        catch (char const * const ex) {
            caught = true;
            if (strcmp(ex, test[i][3])) {
                BOOST_MESSAGE(test[i][0] << " " << test[i][1] <<
                              " " << test[i][2] << " expected: |"
                              << test[i][3] << "| got: |" << ex << "|");
                BOOST_CHECK(0);
            }
        }
        catch (boost::bad_expression badexp) {
            // regex format problem
            BOOST_MESSAGE("got boost exception " << test[i][3]);
            if (!strcmp("regex", test[i][3])) {
                BOOST_MESSAGE("setting caught to true");
                caught = true;
            } else {
                BOOST_MESSAGE("unexpected regex exception: "
                              << badexp.what());
                BOOST_CHECK(0);
            }
        }
        catch (...) {
            // unexpected exception
            BOOST_MESSAGE("unexpected unknown exception");
            BOOST_CHECK(0);
        }

        if (!caught) {
            BOOST_CHECK(0);
        }
    }
}


FENNEL_UNIT_TEST_SUITE(SqlRegExpTest);

// End SqlRegExpTest.cpp
