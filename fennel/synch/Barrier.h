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

#ifndef Fennel_Barrier_Included
#define Fennel_Barrier_Included

#include "fennel/synch/SynchMonitoredObject.h"
#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Barrier implements the standard barrier synchronization primitive.
 */
class Barrier : public SynchMonitoredObject
{
    uint nThreadsWaiting;
    uint nThreadsExpected;
    
public:

    /**
     * Constructs a new barrier.  Initially, the barrier is not ready to receive
     * threads; reset must be called first.
     */
    explicit Barrier();

    /**
     * Destroys this barrier.  Illegal if there are any threads still
     * waiting.
     */
    ~Barrier();

    /**
     * Resets the barrier.  Illegal if there are any threads already
     * waiting.
     *
     * @param nThreadsExpected the number of threads which must call waitFor
     * before all are released
     */
    void reset(uint nThreadsExpected);
    
    /**
     * Waits for the expected number of threads to arrive.  Illegal if
     * the barrier has not yet been reset.
     */
    void waitFor();
};

FENNEL_END_NAMESPACE

#endif

