/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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
package org.eigenbase.relopt;

import java.util.*;


/**
 * MulticastRelOptListener implements the {@link RelOptListener} interface by
 * forwarding events on to a collection of other listeners.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MulticastRelOptListener
    implements RelOptListener
{
    //~ Instance fields --------------------------------------------------------

    private final List<RelOptListener> listeners;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new empty multicast listener.
     */
    public MulticastRelOptListener()
    {
        listeners = new ArrayList<RelOptListener>();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Adds a listener which will receive multicast events.
     *
     * @param listener listener to add
     */
    public void addListener(RelOptListener listener)
    {
        listeners.add(listener);
    }

    // implement RelOptListener
    public void relEquivalenceFound(RelEquivalenceEvent event)
    {
        for (RelOptListener listener : listeners) {
            listener.relEquivalenceFound(event);
        }
    }

    // implement RelOptListener
    public void ruleAttempted(RuleAttemptedEvent event)
    {
        for (RelOptListener listener : listeners) {
            listener.ruleAttempted(event);
        }
    }

    // implement RelOptListener
    public void ruleProductionSucceeded(RuleProductionEvent event)
    {
        for (RelOptListener listener : listeners) {
            listener.ruleProductionSucceeded(event);
        }
    }

    // implement RelOptListener
    public void relChosen(RelChosenEvent event)
    {
        for (RelOptListener listener : listeners) {
            listener.relChosen(event);
        }
    }

    // implement RelOptListener
    public void relDiscarded(RelDiscardedEvent event)
    {
        for (RelOptListener listener : listeners) {
            listener.relDiscarded(event);
        }
    }
}

// End MulticastRelOptListener.java
