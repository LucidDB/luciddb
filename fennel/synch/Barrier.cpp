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
#include "fennel/synch/Barrier.h"

FENNEL_BEGIN_CPPFILE("$Id$");

Barrier::Barrier()
{
    nThreadsExpected = 0;
    nThreadsWaiting = 0;
}

Barrier::~Barrier()
{
    assert(!nThreadsWaiting);
}

void Barrier::reset(uint nThreadsExpectedInit)
{
    assert(!nThreadsWaiting);
    assert(nThreadsExpectedInit);
    nThreadsExpected = nThreadsExpectedInit;
}

void Barrier::waitFor()
{
    assert(nThreadsExpected);
    StrictMutexGuard guard(mutex);
    ++nThreadsWaiting;
    if (nThreadsWaiting == nThreadsExpected) {
        nThreadsWaiting = 0;
        nThreadsExpected = 0;
        condition.notify_all();
    } else {
        while (nThreadsExpected) {
            condition.wait(guard);
        }
    }
}

FENNEL_END_CPPFILE("$Id$");

// End Barrier.cpp
