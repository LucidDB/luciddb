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
#include "fennel/synch/Thread.h"
#include <boost/thread/thread.hpp>
#include <boost/bind.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

// NOTE: it's important to realize that pBoostThread and bRunning are not
// redundant.  The reason is that pBoostThread is updated in the context of the
// thread calling start(), while bRunning is updated in the
// context of the spawned thread.

Thread::Thread(std::string const & desc)
{
    // TODO:  do something with description
    name = desc;
    pBoostThread = NULL;
    bRunning = false;
}

Thread::~Thread()
{
    assert(!bRunning);
    assert(!pBoostThread);
}

void Thread::start()
{
    pBoostThread = new boost::thread(
        boost::bind(&Thread::initAndRun,this));
}

void Thread::join()
{
    assert(pBoostThread);
    assert(*pBoostThread != boost::thread());
    pBoostThread->join();
    delete pBoostThread;
    pBoostThread = NULL;
}

void Thread::initAndRun()
{
    beforeRun();
    run();
    afterRun();
}

void Thread::beforeRun()
{
    bRunning = true;
}

void Thread::afterRun()
{
    bRunning = false;
}

FENNEL_END_CPPFILE("$Id$");

// End Thread.cpp
