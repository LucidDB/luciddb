/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
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
#include "fennel/synch/GroupLock.h"
 
FENNEL_BEGIN_CPPFILE("$Id$");

GroupLock::GroupLock()
{
    nHolders = 0;
    iHeldGroup = 0;
}

GroupLock::~GroupLock()
{
    assert(!nHolders);
}

bool GroupLock::waitFor(uint iGroup,uint iTimeout)
{
    boost::xtime atv;
    convertTimeout(iTimeout,atv);
    StrictMutexGuard mutexGuard(mutex);
    while (nHolders && iHeldGroup != iGroup) {
        if (!condition.timed_wait(mutexGuard,atv)) {
            return false;
        }
    }
    nHolders++;
    iHeldGroup = iGroup;
    return true;
}

void GroupLock::release()
{
    StrictMutexGuard mutexGuard(mutex);
    assert(nHolders);
    if (!--nHolders) {
        iHeldGroup = 0;
        condition.notify_all();
    }
}


FENNEL_END_CPPFILE("$Id$");

// End GroupLock.cpp
