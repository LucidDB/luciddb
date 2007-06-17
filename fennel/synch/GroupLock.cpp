/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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
