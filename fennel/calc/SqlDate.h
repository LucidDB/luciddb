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
//
// SqlString
//
// An ASCII & UCS2 string library that adheres to the SQL99 standard definitions
*/
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
//
// SqlDate
//
// Sql date/time/timestamp functionality.
*/
#ifndef Fennel_SqlDate_Included
#define Fennel_SqlDate_Included

// this include needs to come first, since some int64 macros in ICU conflicts
// with the boost ones.  Fortunately ICU is smart enough.
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian_types.hpp>
#include <boost/date_time/c_local_time_adjustor.hpp>

#ifdef HAVE_ICU
#include <unicode/ustring.h>
#endif


FENNEL_BEGIN_NAMESPACE

#if !(defined LITTLEENDIAN || defined BIGENDIAN)
#error "endian not defined"
#endif

/** \file SqlDate.h 
 *
 * SqlDate
 *
 */

enum SqlDateTimeType { SQLDATE, SQLTIME, SQLTIMESTAMP };

template <int CodeUnitBytes, int MaxCodeUnitsPerCodePoint, SqlDateTimeType dateTimeType>
int
SqlDateToStr(char *dest,
             int destStorageBytes,
             int64_t const d )
{
    using namespace boost::posix_time; 
    using namespace boost::gregorian;

    typedef boost::date_time::c_local_adjustor<boost::posix_time::ptime> local_adj;
    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint) {
        if (CodeUnitBytes == 1) {
            // ASCII
            int64_t tt = d;
            // from_time_t isn't in the version of boost we're using. sigh.
            //          boost::posix_time::ptime t = boost::posix_time::from_time_t(tt);
            ptime const epoc(date(1970,Jan,1));
            // we could use the millisecond() duration constructor,
            // instead of time_duration(...), but the time_duration was
            // the only way i could find didn't use an explicit long
            // paramter, instead of the type parameter, since 
            // int64_t == (long long) on (fc1) linux.
            boost::posix_time::ptime t = epoc + time_duration(0,0,0,tt);

            // deal with UTC conversion.  This is bogus - we should get the session tz from
            // the environment somehow.
            t = local_adj::utc_to_local(t);

            //          cout << boost::gregorian::to_iso_extended_string(t.date()) << endl;
            int len;
            if (dateTimeType == SQLDATE) {
                char buf[11];           // extra byte for NUL
                len = DateToIsoString(buf, t);
                if (len > destStorageBytes) {
                    // SQL99 6.22, general rule 9 case a.iii => 
                    // exception SQL99 22.1 22-001 "String Data Right truncation"
                    throw "22001"; 
                }
                memcpy(dest,buf,len);
            } else if (dateTimeType == SQLTIME) {
                char buf[9];           // extra byte for NUL
                len = TimeToIsoString(buf, t);
                if (len > destStorageBytes) {
                    // SQL99 6.22, general rule 9 case a.iii => 
                    // exception SQL99 22.1 22-001 "String Data Right truncation"
                    throw "22001"; 
                }
                memcpy(dest,buf,len);
            } else if (dateTimeType == SQLTIMESTAMP) {
                char buf[20];           // extra byte for NUL
                len = TimestampToIsoString(buf, t);
                if (len > destStorageBytes) {
                    // SQL99 6.22, general rule 9 case a.iii => 
                    // exception SQL99 22.1 22-001 "String Data Right truncation"
                    throw "22001"; 
                }
                memcpy(dest,buf,len);
            }


            return len;
        } else if (CodeUnitBytes == 2) {
            // TODO: Add UCS2 here
            throw std::logic_error("no UCS2");
        } else {
            throw std::logic_error("no such encoding");
        }
    } else {
        throw std::logic_error("no UTF8/16/32");
    }
   
}


int TimeToIsoString(char *dest, boost::posix_time::ptime t);
int DateToIsoString(char *dest, boost::posix_time::ptime t);
int TimestampToIsoString(char *dest, boost::posix_time::ptime t);
FENNEL_END_NAMESPACE

#endif
