/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

FENNEL_BEGIN_NAMESPACE

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


int64_t IsoStringToDate(char *src, int len)
{
    std::string s(src,len);
  

    date_duration td = boost::gregorian::from_string(s) - epoc.date();
  
    return td.days() * milliseconds_per_day;
  
  
}

int64_t IsoStringToTime(char *src, int len) 
{
    std::string s(src, len);
    
    time_duration td = duration_from_string(s);
    //    ptime p = epoc + td;
    
    return td.ticks()/1000;
    
}

int64_t IsoStringToTimestamp(char *src, int len) 
{

    std::string s(src, len);
    
    ptime p(time_from_string(s));
    
    time_duration td = p - epoc;
    
    return td.ticks()/1000;

}

int64_t CurrentTime()
{
    ptime p = second_clock::local_time();
    return p.time_of_day().ticks()/1000;
    
}

int64_t CurrentTimestamp()
{
    ptime p = second_clock::local_time();
    return (p - epoc).ticks()/1000;
    
}


FENNEL_END_NAMESPACE

