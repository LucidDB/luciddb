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

#ifndef Fennel_StatsTarget_Included
#define Fennel_StatsTarget_Included

FENNEL_BEGIN_NAMESPACE

/**
 * StatsTarget defines an interface implemented by classes which consume
 * performance/activity statistics.
 */
class StatsTarget
{
public:
    virtual ~StatsTarget();

    /**
     * Begin recording a snapshot.  Called before all writeCounter invocations
     * for the snapshot.
     */
    virtual void beginSnapshot() = 0;
    
    /**
     * Finish recording a snapshot.  Called after all writeCounter invocations
     * for the snapshot.
     */
    virtual void endSnapshot() = 0;
    
    /**
     * Write one uint counter.  This is called from a StatsSource
     * implementation in response to writeStats().
     *
     * @param name name of counter
     *
     * @param value snapshot value
     */
    virtual void writeCounter(std::string name,uint value) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End StatsTarget.h
