/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2008-2009 The Eigenbase Project
// Copyright (C) 2008-2009 SQLstream, Inc.
// Copyright (C) 2008-2009 LucidEra, Inc.
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

#ifndef Fennel_ThreadTracker_Included
#define Fennel_ThreadTracker_Included

FENNEL_BEGIN_NAMESPACE

class FennelExcn;

/**
 * ThreadTracker defines an interface for receiving callbacks
 * before and after a thread runs.  The default implementation
 * is a dummy (stub methods doing nothing).
 *
 * @author John Sichi
 * @version $Id$
 */
class ThreadTracker
{
public:
    virtual ~ThreadTracker();

    /**
     * Called in new thread context before thread's body runs.
     */
    virtual void onThreadStart();

    /**
     * Called in thread context after thread's body runs.
     */
    virtual void onThreadEnd();

    /**
     * Clones an exception so that it can be rethrown in
     * a different thread context.
     *
     * @param ex the excn to be cloned
     *
     * @return cloned excn
     */
    virtual FennelExcn *cloneExcn(std::exception &ex);
};

FENNEL_END_NAMESPACE

#endif

// End ThreadTracker.h
