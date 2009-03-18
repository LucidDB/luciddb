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

#ifndef Fennel_GroupLock_Included
#define Fennel_GroupLock_Included

#include "fennel/synch/SynchMonitoredObject.h"

FENNEL_BEGIN_NAMESPACE

/**
 * GroupLock is a synchronization object for enforcing mutual exclusion among
 * an indefinite number of groups with indefinite cardinalities.  As an
 * example, suppose you only had a single bathroom, and you wanted to prevent
 * members of the opposite sex from occupying it simultaneously.  You could do
 * this by slapping a GroupLock on the door; men would enter with group key "1"
 * and women would enter with group key "2"; the GroupLock would allow any
 * number of men to enter together, or any number of women, but would never
 * allow them to mix.  In this case, there are only two groups, but any number
 * of groups is supported, as long as they have unique integer identifiers.
 * Note that there are no provisions for preventing starvation, or whatever the
 * equally unpleasant equivalent is in this example.
 */
class GroupLock : public SynchMonitoredObject
{
    uint nHolders;
    uint iHeldGroup;

public:
    explicit GroupLock();
    ~GroupLock();

    bool waitFor(uint iGroup,uint iTimeout = ETERNITY);

    /**
     * // TODO:  pass the group key to release as well,
     * and assert that it matches iHeldGroup
     */
    void release();
};

FENNEL_END_NAMESPACE

#endif

// End GroupLock.h
