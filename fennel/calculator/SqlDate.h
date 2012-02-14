/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
#ifndef Fennel_SqlDate_Included
#define Fennel_SqlDate_Included

// this include needs to come first, since some int64 macros in ICU conflicts
// with the boost ones.  Fortunately ICU is smart enough.
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/date_time/gregorian/gregorian_types.hpp>
#include <boost/date_time/c_local_time_adjustor.hpp>

#ifdef HAVE_ICU
#include <unicode/ustring.h>
#endif

#include "fennel/calculator/SqlState.h"

FENNEL_BEGIN_NAMESPACE

#if !(defined LITTLEENDIAN || defined BIGENDIAN)
#error "endian not defined"
#endif

/** \file SqlDate.h
 *
 * SqlDate
 *
 */

enum SqlDateTimeType {
    SQLDATE,
    SQLTIME,
    SQLTIMESTAMP
};

boost::posix_time::ptime const epoc(boost::gregorian::date(1970, 1, 1));

int FENNEL_CALCULATOR_EXPORT TimeToIsoString(
    char *dest, boost::posix_time::ptime t);
int FENNEL_CALCULATOR_EXPORT DateToIsoString(
    char *dest, boost::posix_time::ptime t);
int FENNEL_CALCULATOR_EXPORT TimestampToIsoString(
    char *dest, boost::posix_time::ptime t);

int64_t FENNEL_CALCULATOR_EXPORT IsoStringToTime(
    char const * const src, int len);
int64_t FENNEL_CALCULATOR_EXPORT IsoStringToDate(
    char const * const src, int len);
int64_t FENNEL_CALCULATOR_EXPORT IsoStringToTimestamp(
    char const * const src, int len);

template <
    int CodeUnitBytes,
    int MaxCodeUnitsPerCodePoint,
    SqlDateTimeType dateTimeType>
int
SqlDateToStr(
    char *dest,
    int destStorageBytes,
    int64_t const d,
    bool fixed = false,  // e.g. char, else variable (varchar)
    int padchar = ' ')
{
    using namespace boost::posix_time;
    using namespace boost::gregorian;

    typedef boost::date_time::c_local_adjustor<boost::posix_time::ptime>
        local_adj;
    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint) {
        if (CodeUnitBytes == 1) {
            // ASCII

            // from_time_t isn't in the version of boost we're using. sigh.
            // FIXME: jhyde: No longer true. Let's use it.
            //   boost::posix_time::ptime t = boost::posix_time::from_time_t(d);

            // we could use the millisecond() duration constructor,
            // instead of time_duration(...), but the time_duration was
            // the only way i could find didn't use an explicit long
            // parameter, instead of the type parameter, since
            // int64_t == (long long) on (fc1) linux.
            boost::posix_time::ptime t = epoc + time_duration(0, 0, 0, d);

            int len;
            char buf[20];
            switch (dateTimeType) {
            case SQLDATE:
                len = DateToIsoString(buf, t);
                if (len > destStorageBytes) {
                    // SQL99 Part 2 Section 6.22 General Rule 9.a.iii =>
                    // exception SQL99 22.1 22-001 "String Data Right
                    // truncation"
                    throw SqlState::instance().code22001();
                }
                memcpy(dest, buf, len);
                break;
            case SQLTIME:
                len = TimeToIsoString(buf, t);
                if (len > destStorageBytes) {
                    // SQL99 Part 2 Section 6.22 General Rule 9.a.iii =>
                    // exception SQL99 22.1 22-001 "String Data Right
                    // truncation"
                    throw SqlState::instance().code22001();
                }
                memcpy(dest, buf, len);
                break;
            case SQLTIMESTAMP:
                len = TimestampToIsoString(buf, t);
                if (len > destStorageBytes) {
                    // SQL99 Part 2 Section 6.22 General Rule 9.a.iii =>
                    // exception SQL99 22.1 22-001 "String Data Right
                    // truncation"
                    throw SqlState::instance().code22001();
                }
                memcpy(dest, buf, len);
                break;
            default:
                throw std::logic_error("bad dateTimeType" + dateTimeType);
            }

            if (fixed) {
                memset(dest + len, padchar, destStorageBytes - len);
                return destStorageBytes;
            } else {
                return len;
            }
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

template <
    int CodeUnitBytes,
    int MaxCodeUnitsPerCodePoint,
    SqlDateTimeType dateTimeType>
int64_t
SqlStrToDate(char *src, int len)
{
    using namespace boost::posix_time;
    using namespace boost::gregorian;

    if (CodeUnitBytes == MaxCodeUnitsPerCodePoint) {
        if (CodeUnitBytes == 1) {
            // ASCII

            switch (dateTimeType) {
            case SQLDATE:
                return IsoStringToDate(src, len);
            case SQLTIME:
                return IsoStringToTime(src, len);
            case SQLTIMESTAMP:
                return IsoStringToTimestamp(src, len);
            }
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

/// Returns time of day in UTC.
///
/// (This function used to be called CurrentTime, but as FNL-77 points out,
/// that is misleading, because CurrentTime's result is in the local timezone.)
int64_t FENNEL_CALCULATOR_EXPORT UniversalTime();

/// Returns timestamp in UTC. That is, milliseconds since 1970-1-1 00:00:00 UTC.
///
/// (This function used to be called CurrentTimestamp, but as FNL-77 points out,
/// that is misleading, because CurrentTimestamp's result is in the local
/// timezone.)
int64_t FENNEL_CALCULATOR_EXPORT UniversalTimestamp();

/// Returns the time of day in the given time zone.
int64_t FENNEL_CALCULATOR_EXPORT LocalTime(
    boost::local_time::time_zone_ptr tzPtr);

/// Returns the timestamp in the given time zone.
int64_t FENNEL_CALCULATOR_EXPORT LocalTimestamp(
    boost::local_time::time_zone_ptr tzPtr);

FENNEL_END_NAMESPACE

#endif

// End SqlDate.h
