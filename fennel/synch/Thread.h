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

#ifndef Fennel_Thread_Included
#define Fennel_Thread_Included

#include "fennel/synch/SynchObj.h"
#include <boost/utility.hpp>

namespace boost 
{
class thread;
};

FENNEL_BEGIN_NAMESPACE

/**
 * Thread is a wrapper around boost::thread which allows for the thread object
 * to be created before it is actually started.
 */
class Thread : public boost::noncopyable
{
protected:
    boost::thread *pBoostThread;
    bool bRunning;
    std::string name;
    
    void initAndRun();
    virtual void run() = 0;
    virtual void beforeRun();
    virtual void afterRun();
    
public:
    explicit Thread(std::string const &description = "anonymous thread");
    virtual ~Thread();

    /**
     * Spawns the OS thread.
     */
    virtual void start();

    /**
     * Waits for the OS thread to terminate.
     */
    void join();

    /**
     * @return true if start has been called (and subsequent join has not
     * completed)
     */
    bool isStarted() const
    {
        return pBoostThread ? true : false;
    }

    /**
     * @return opposite of isStarted()
     */
    bool isStopped() const
    {
        return !isStarted();
    }

    /**
     * Accesses the underlying boost::thread, e.g. for use in a
     * boost::thread_group.  This thread must already be started.
     *
     * @return the underlying boost::thread
     */
    boost::thread &getBoostThread()
    {
        assert(isStarted());
        return *pBoostThread;
    }

    std::string getName()
    {
        return name;
    }

    void setName(std::string const &s)
    {
        name = s;
    }
};

FENNEL_END_NAMESPACE

#endif

// End Thread.h
