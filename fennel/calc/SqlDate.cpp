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
// SqlRegExp
//
// An ASCII & UCS2 string library that adheres to the SQL99 standard definitions
// and implements LIKE and SIMILAR.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/calc/SqlDate.h"

FENNEL_BEGIN_NAMESPACE

int DateToIsoString(char *dest, boost::posix_time::ptime t)
{
  using namespace boost::posix_time; 
  using namespace boost::gregorian;

  int y = t.date().year();
  int m =  t.date().month();
  int dy = t.date().day();
  return snprintf(dest,11, "%4d-%02d-%02d", y,m, dy);


}


int TimeToIsoString(char *dest, boost::posix_time::ptime t)
{
  using namespace boost::posix_time; 
  using namespace boost::gregorian;

  time_duration td = t.time_of_day();
  int h = td.hours();
  int m =  td.minutes();
  int s = td.seconds();
  return snprintf(dest,9, "%02d:%02d:%02d", h, m, s);


}

int TimestampToIsoString(char *dest, boost::posix_time::ptime t)
{
  using namespace boost::posix_time; 
  using namespace boost::gregorian;

  time_duration td = t.time_of_day();
  int h = td.hours();
  int min =  td.minutes();
  int s = td.seconds();

  int y = t.date().year();
  int mon =  t.date().month();
  int dy = t.date().day();
  return snprintf(dest,20, "%4d-%02d-%02d %02d:%02d:%02d", y,mon, dy, h, min, s);


}




FENNEL_END_NAMESPACE

