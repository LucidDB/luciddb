/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
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

/**
 * SqlDate functions.  These use the boost posix_time and gregorian
 * calendars.  However, its pretty yucky - the current date_time lib
 * doesn't provide convenient access to the millisec time, and so we
 * convert to durations by subtracting the epoc.  Also, the underlying
 * rep is microsecond, so we divide by 1000 to get milliseconds (which
 * is what java uses).
 *
 *  we should probably use the templates explicitly to implement our own class.
 */

#include "fennel/common/CommonPreamble.h"
#include "fennel/calculator/SqlDate.h"
#include "boost/date_time/gregorian/parsers.hpp"
#include "boost/regex.hpp"

FENNEL_BEGIN_NAMESPACE

using namespace boost;
using namespace boost::posix_time;
using namespace boost::local_time;
using namespace boost::gregorian;

typedef boost::date_time::c_local_adjustor<boost::posix_time::ptime> local_adj;

int DateToIsoString(char *dest, boost::posix_time::ptime t)
{
    int y = t.date().year();
    int m =  t.date().month();
    int dy = t.date().day();
    return snprintf(dest,11, "%4d-%02d-%02d", y,m, dy);
}


int TimeToIsoString(char *dest, boost::posix_time::ptime t)
{
    time_duration td = t.time_of_day();
    int h = td.hours();
    int m = td.minutes();
    int s = td.seconds();
    return snprintf(dest,9, "%02d:%02d:%02d", h, m, s);
}

int TimestampToIsoString(char *dest, boost::posix_time::ptime t)
{
    time_duration td = t.time_of_day();
    int h   = td.hours();
    int min = td.minutes();
    int s   = td.seconds();

    int y   = t.date().year();
    int mon = t.date().month();
    int dy  = t.date().day();
    return snprintf(dest,20, "%4d-%02d-%02d %02d:%02d:%02d", y,mon, dy, h, min, s);
}

int64_t milliseconds_per_day = 24 * 60 * 60 * 1000LL;

// NOTE jvs 14-Aug-2005:  I added this as part of upgrading to Boost 1.33
// because the datetime library is no longer forgiving of spaces.
static inline void trimSpaces(std::string &s)
{
    std::string::size_type n = s.find_last_not_of(' ');

    if (n == std::string::npos) {
        s.resize(0);
    } else {
        s.resize(n + 1);
        n = s.find_first_not_of(' ');
        s.erase(0, n);
    }
}

int64_t IsoStringToDate(const char * const src, int len)
{
    std::string s(src,len);
    trimSpaces(s);

    regex dateExp("\\d+-\\d+-\\d+");
    if (regex_match(s, dateExp)) {
        try {
            date_duration td = boost::gregorian::from_string(s) - epoc.date();
            return td.days() * milliseconds_per_day;
        } catch (...) {
            // Fall through to throw
        }
    }

    // Parse of date failed
    // SQL2003 Part 2 Section 6.12 General Rule 13 data
    // exception -- invalid datetime format
    throw "22007";
}

int64_t IsoStringToTime(const char * const src, int len)
{
    std::string s(src, len);
    trimSpaces(s);

    // TODO: Boost library doesn't catch invalid hour, min, sec
    // TODO: Try updated boost library to see if we can get
    // TODO: rid of this tiresome check
    cmatch what;
    regex timeExp("(\\d+):(\\d+):(\\d+)(\\.\\d+)?");
    if (regex_match(s.c_str(), what, timeExp)) {
        try {
            int hour = atoi(what[1].first);
            int min = atoi(what[2].first);
            int sec = atoi(what[3].first);

            if ((hour >= 0) && (hour < 24) &&
                (min >= 0) && (min < 60) &&
                (sec >= 0) && (sec < 60)) {
                time_duration td = duration_from_string(s);
                return td.total_milliseconds();
            }
        } catch (...) {
            // Fall through to throw
        }
    }

    // Parse of time failed
    // SQL2003 Part 2 Section 6.12 General Rule 15,16 data
    // exception -- invalid datetime format
    throw "22007";
}

int64_t IsoStringToTimestamp(const char * const src, int len)
{

    std::string s(src, len);
    trimSpaces(s);

    // TODO: Boost library doesn't catch invalid hour, min, sec
    // TODO: Try updated boost library to see if we can get
    // TODO: rid of this tiresome check
    cmatch what;
    regex timestampExp("\\d+-\\d+-\\d+ +"
                       "(\\d+):(\\d+):(\\d+)(\\.\\d+)?");
    if (regex_match(s.c_str(), what, timestampExp)) {
        try {
            int hour = atoi(what[1].first);
            int min = atoi(what[2].first);
            int sec = atoi(what[3].first);

            if ((hour >= 0) && (hour < 24) &&
                (min >= 0) && (min < 60) &&
                (sec >= 0) && (sec < 60)) {
                ptime p(time_from_string(s));
                time_duration td = p - epoc;
                return td.total_milliseconds();
            }
        } catch (...) {
            // Fall through to throw
        }
    }

    // Parse of timestamp failed
    // SQL2003 Part 2 Section 6.12 General Rule 17,18 data
    // exception -- invalid datetime format
    throw "22007";
}

int64_t UniversalTime()
{
    ptime p = second_clock::universal_time();
    return p.time_of_day().total_milliseconds();
}

int64_t UniversalTimestamp()
{
    // REVIEW: SWZ: 4/30/2006: In practice, we should return the micro
    // second delta (or as much precision as we can muster) and let
    // the instruction (which may have been given an explicit
    // precision) truncate the fractional seconds.  For now, returning
    // millis causes Fennel Calc to behave like the Java Calc for
    // CURRENT_TIME[STAMP].
    ptime p = microsec_clock::universal_time();
    return (p - epoc).total_milliseconds();
}

int64_t LocalTime(boost::local_time::time_zone_ptr tzPtr)
{
    local_date_time plocal = local_microsec_clock::local_time(tzPtr);
    return plocal.local_time().time_of_day().total_milliseconds();
}

int64_t LocalTimestamp(boost::local_time::time_zone_ptr tzPtr)
{
    // Create a local epoch. For PST, for example, the epoch is 1970/1/1 PST,
    // which occurred 8 hrs after the UTC epoch.
    date d(1970, 1, 1);
    time_duration td(0, 0, 0);
    local_date_time local_epoc(
        d, td, tzPtr, local_date_time::NOT_DATE_TIME_ON_ERROR);

    local_date_time plocal = local_microsec_clock::local_time(tzPtr);
    time_duration diff = plocal - local_epoc;

    // Adjust the difference if we are now in DST and the epoch is not, or vice
    // versa.
    if (plocal.is_dst()) {
        if (local_epoc.is_dst()) {
            // same offset: nothing to do
        } else {
            diff += tzPtr->dst_offset();
        }
    } else {
        if (local_epoc.is_dst()) {
            diff -= tzPtr->dst_offset();
        } else {
            // same offset: nothing to do
        }
    }
    return diff.total_milliseconds();
}


FENNEL_END_NAMESPACE

// End SqlDate.cpp
