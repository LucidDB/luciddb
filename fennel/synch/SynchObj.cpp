/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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
#include "fennel/synch/SynchObj.h"
#include "fennel/synch/Thread.h"
#include "fennel/synch/NullMutex.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void convertTimeout(uint iMilliseconds, boost::xtime &atv)
{
    boost::xtime_get(&atv,boost::TIME_UTC);
    if (isMAXU(iMilliseconds)) {
        // FIXME:  Solaris doesn't like bogus huge times like
        // ACE_Time_Value::max_time, so this uses NOW+10HRS.  Instead, should
        // precalculate a real time much farther in the future and keep it
        // around as a singleton.
        atv.sec += 36000;
    } else if (iMilliseconds) {
        long sec = iMilliseconds / 1000;
        long nsec = (iMilliseconds % 1000) * 1000000;
        atv.sec += sec;
        atv.nsec += nsec;
    }
}

// force references to some classes which aren't referenced elsewhere
#ifdef __MSVC__
class UnreferencedSynchStructs
{
    NullMutex nullMutex;
};
#endif

FENNEL_END_CPPFILE("$Id$");

// End SynchObj.cpp
