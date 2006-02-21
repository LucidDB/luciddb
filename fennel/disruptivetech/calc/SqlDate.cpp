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
#include "fennel/disruptivetech/calc/SqlDate.h"
#include "boost/date_time/gregorian/parsers.hpp"
#include "boost/regex.hpp"

FENNEL_BEGIN_NAMESPACE

using namespace boost;
using namespace boost::posix_time;
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

    regex dateExp("\\d\\d\\d\\d-\\d\\d-\\d\\d");
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
    regex timeExp("(\\d\\d):(\\d\\d):(\\d\\d)(\\.\\d+)?");
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
    regex timestampExp("\\d\\d\\d\\d-\\d\\d-\\d\\d +"
                       "(\\d\\d):(\\d\\d):(\\d\\d)(\\.\\d+)?");
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

int64_t CurrentTime()
{
    ptime p = second_clock::universal_time();
    return p.time_of_day().total_milliseconds();
    
}

int64_t CurrentTimestamp()
{
    ptime p = second_clock::universal_time();
    return (p - epoc).total_milliseconds();
}


FENNEL_END_NAMESPACE

