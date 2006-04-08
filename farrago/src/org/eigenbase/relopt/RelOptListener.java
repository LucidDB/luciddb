/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2005-2005 John V. Sichi
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

import org.eigenbase.rel.*;
import java.util.*;

/**
 * RelOptListener defines an interface for listening to events which
 * occur during the optimization process.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface RelOptListener extends EventListener
{
    /**
     * Notifies this listener that a relational expression has been registered
     * with a particular equivalence class after an equivalence has been either
     * detected or asserted.  Equivalence classes may be either
     * logical (all expressions which yield the same result set) or
     * physical (all expressions which yield the same result set
     * with a particular calling convention).
     *
     * @param event details about the event
     */
    public void relEquivalenceFound(RelEquivalenceEvent event);

    /**
     * Notifies this listener that an optimizer rule is being applied to a
     * particular relational expression.  This rule is called twice; once
     * before the rule is invoked, and once after.  Note that the rel attribute
     * of the event is always the old expression.
     *
     * @param event details about the event
     */
    public void ruleAttempted(RuleAttemptedEvent event);

    /**
     * Notifies this listener that an optimizer rule has been successfully
     * applied to a particular relational expression, resulting in a new
     * equivalent expression (relEquivalenceFound will also be called unless
     * the new expression is identical to an existing one).  This rule is
     * called twice; once before registration of the new rel, and once after.
     * Note that the rel attribute of the event is always the new expression;
     * to get the old expression, use event.getRuleCall().rels[0].
     *
     * @param event details about the event
     */
    public void ruleProductionSucceeded(RuleProductionEvent event);

    /**
     * Notifies this listener that a relational expression is no
     * longer of interest to the planner.
     *
     * @param event details about the event
     */
    public void relDiscarded(RelDiscardedEvent event);
    
    /**
     * Notifies this listener that a relational expression has been
     * chosen as part of the final implementation of the query plan.
     * After the plan is copmlete, this is called one more time
     * with null for the rel.
     *
     * @param event details about the event
     */
    public void relChosen(RelChosenEvent event);
    
    /**
     * Event class for abstract event dealing with a relational expression.
     * The source of an event is typically the RelOptPlanner which initiated
     * it.
     */
    public static abstract class RelEvent extends EventObject
    {
        private final RelNode rel;

        protected RelEvent(Object eventSource, RelNode rel)
        {
            super(eventSource);
            this.rel = rel;
        }

        public RelNode getRel()
        {
            return rel;
        }
    }

    public static class RelChosenEvent extends RelEvent 
    {
        public RelChosenEvent(Object eventSource, RelNode rel)
        {
            super(eventSource, rel);
        }
    }
    
    public static class RelEquivalenceEvent extends RelEvent
    {
        private final Object equivalenceClass;
        private final boolean isPhysical;

        public RelEquivalenceEvent(
            Object eventSource, RelNode rel, Object equivalenceClass,
            boolean isPhysical)
        {
            super(eventSource, rel);
            this.equivalenceClass = equivalenceClass;
            this.isPhysical = isPhysical;
        }

        public Object getEquivalenceClass()
        {
            return equivalenceClass;
        }

        public boolean isPhysical()
        {
            return isPhysical;
        }
    }

    public static class RelDiscardedEvent extends RelEvent
    {
        public RelDiscardedEvent(Object eventSource, RelNode rel)
        {
            super(eventSource, rel);
        }
    }
    
    public static abstract class RuleEvent extends RelEvent
    {
        private final RelOptRuleCall ruleCall;

        protected RuleEvent(
            Object eventSource, RelNode rel, RelOptRuleCall ruleCall)
        {
            super(eventSource, rel);
            this.ruleCall = ruleCall;
        }

        public RelOptRuleCall getRuleCall()
        {
            return ruleCall;
        }
    }

    public static class RuleAttemptedEvent extends RuleEvent
    {
        private final boolean before;
        
        public RuleAttemptedEvent(
            Object eventSource, RelNode rel, RelOptRuleCall ruleCall,
            boolean before)
        {
            super(eventSource, rel, ruleCall);
            this.before = before;
        }

        public boolean isBefore()
        {
            return before;
        }
    }

    public static class RuleProductionEvent extends RuleAttemptedEvent
    {
        public RuleProductionEvent(
            Object eventSource, RelNode rel, RelOptRuleCall ruleCall,
            boolean before)
        {
            super(eventSource, rel, ruleCall, before);
        }
    }
}

// End RelOptListener.java
