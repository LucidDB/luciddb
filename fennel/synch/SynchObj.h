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

#ifndef Fennel_SynchObj_Included
#define Fennel_SynchObj_Included

#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/xtime.hpp>
#include <boost/thread/condition.hpp>

FENNEL_BEGIN_NAMESPACE

#ifdef __CYGWIN__
// NOTE:  This is a stupid hack to account for the fact that on Cygwin,
// pthread_mutex_init may return EBUSY if the mutex initially contains certain
// bit patterns.
template <class BoostMutex>
class FennelMutex 
{
    int dummy;
    BoostMutex boostMutex;

    int zeroBoostMutex()
    {
        memset(&boostMutex,0,sizeof(boostMutex));
        return 0;
    }

public:
    FennelMutex()
        : dummy(zeroBoostMutex())
    {
    }
    
    operator BoostMutex &()
    {
        return boostMutex;
    }
};

typedef FennelMutex<boost::recursive_try_mutex> RecursiveMutex;
typedef FennelMutex<boost::try_mutex> StrictMutex;

#else

typedef boost::recursive_try_mutex RecursiveMutex;
typedef boost::try_mutex StrictMutex;

#endif

typedef boost::recursive_try_mutex::scoped_lock RecursiveMutexGuard;
typedef boost::try_mutex::scoped_lock StrictMutexGuard;
typedef boost::recursive_try_mutex::scoped_try_lock RecursiveMutexTryGuard;
typedef boost::try_mutex::scoped_try_lock StrictMutexTryGuard;
typedef boost::condition LocalCondition;

extern void convertTimeout(uint iMillis,boost::xtime &);

FENNEL_END_NAMESPACE

#endif

// End SynchObj.h
