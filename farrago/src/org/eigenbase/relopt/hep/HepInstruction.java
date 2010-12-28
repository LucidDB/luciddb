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
package org.eigenbase.relopt.hep;

import java.util.*;

import org.eigenbase.relopt.*;


/**
 * HepInstruction represents one instruction in a HepProgram. The actual
 * instruction set is defined here via inner classes; if these grow too big,
 * they should be moved out to top-level classes.
 *
 * @author John V. Sichi
 * @version $Id$
 */
abstract class HepInstruction
{
    //~ Methods ----------------------------------------------------------------

    void initialize(boolean clearCache)
    {
    }

    // typesafe dispatch via the visitor pattern
    abstract void execute(HepPlanner planner);

    //~ Inner Classes ----------------------------------------------------------

    static class RuleClass<R extends RelOptRule>
        extends HepInstruction
    {
        Class<R> ruleClass;

        /**
         * Actual rule set instantiated during planning by filtering all of the
         * planner's rules through ruleClass.
         */
        Set<RelOptRule> ruleSet;

        void initialize(boolean clearCache)
        {
            if (!clearCache) {
                return;
            }

            ruleSet = null;
        }

        void execute(HepPlanner planner)
        {
            planner.executeInstruction(this);
        }
    }

    static class RuleCollection
        extends HepInstruction
    {
        /**
         * Collection of rules to apply.
         */
        Collection<RelOptRule> rules;

        void execute(HepPlanner planner)
        {
            planner.executeInstruction(this);
        }
    }

    static class ConverterRules
        extends HepInstruction
    {
        boolean guaranteed;

        /**
         * Actual rule set instantiated during planning by filtering all of the
         * planner's rules, looking for the desired converters.
         */
        Set<RelOptRule> ruleSet;

        void execute(HepPlanner planner)
        {
            planner.executeInstruction(this);
        }
    }

    static class CommonRelSubExprRules
        extends HepInstruction
    {
        Set<RelOptRule> ruleSet;

        void execute(HepPlanner planner)
        {
            planner.executeInstruction(this);
        }
    }

    static class RuleInstance
        extends HepInstruction
    {
        /**
         * Description to look for, or null if rule specified explicitly.
         */
        String ruleDescription;

        /**
         * Explicitly specified rule, or rule looked up by planner from
         * description.
         */
        RelOptRule rule;

        void initialize(boolean clearCache)
        {
            if (!clearCache) {
                return;
            }

            if (ruleDescription != null) {
                // Look up anew each run.
                rule = null;
            }
        }

        void execute(HepPlanner planner)
        {
            planner.executeInstruction(this);
        }
    }

    static class MatchOrder
        extends HepInstruction
    {
        HepMatchOrder order;

        void execute(HepPlanner planner)
        {
            planner.executeInstruction(this);
        }
    }

    static class MatchLimit
        extends HepInstruction
    {
        int limit;

        void execute(HepPlanner planner)
        {
            planner.executeInstruction(this);
        }
    }

    static class Subprogram
        extends HepInstruction
    {
        HepProgram subprogram;

        void initialize(boolean clearCache)
        {
            subprogram.initialize(clearCache);
        }

        void execute(HepPlanner planner)
        {
            planner.executeInstruction(this);
        }
    }

    static class BeginGroup
        extends HepInstruction
    {
        EndGroup endGroup;

        void initialize(boolean clearCache)
        {
        }

        void execute(HepPlanner planner)
        {
            planner.executeInstruction(this);
        }
    }

    static class EndGroup
        extends HepInstruction
    {
        /**
         * Actual rule set instantiated during planning by collecting grouped
         * rules.
         */
        Set<RelOptRule> ruleSet;

        boolean collecting;

        void initialize(boolean clearCache)
        {
            if (!clearCache) {
                return;
            }

            ruleSet = new HashSet<RelOptRule>();
            collecting = true;
        }

        void execute(HepPlanner planner)
        {
            planner.executeInstruction(this);
        }
    }
}

// End HepInstruction.java
