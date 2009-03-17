/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package org.eigenbase.test;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.hep.*;


/**
 * RelOptTestBase is an abstract base for tests which exercise a planner and/or
 * rules via {@link DiffRepository}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
abstract class RelOptTestBase
    extends SqlToRelTestBase
{
    //~ Methods ----------------------------------------------------------------

    protected abstract DiffRepository getDiffRepos();

    /**
     * Checks the plan for a SQL statement before/after executing a given rule.
     *
     * @param rule Planner rule
     * @param sql SQL query
     */
    protected void checkPlanning(
        RelOptRule rule,
        String sql)
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(rule);

        checkPlanning(
            programBuilder.createProgram(),
            sql);
    }

    /**
     * Checks the plan for a SQL statement before/after executing a given
     * program.
     *
     * @param program Planner program
     * @param sql SQL query
     */
    protected void checkPlanning(
        HepProgram program,
        String sql)
    {
        checkPlanning(
            new HepPlanner(program),
            sql);
    }

    /**
     * Checks the plan for a SQL statement before/after executing a given
     * planner.
     *
     * @param planner Planner
     * @param sql SQL query
     */
    protected void checkPlanning(
        RelOptPlanner planner,
        String sql)
    {
        checkPlanning(
            null,
            planner,
            sql);
    }

    /**
     * Checks the plan for a SQL statement before/after executing a given rule,
     * with a pre-program to prepare the tree.
     *
     * @param preProgram Program to execute before comparing before state
     * @param rule Planner rule
     * @param sql SQL query
     */
    protected void checkPlanning(
        HepProgram preProgram,
        RelOptRule rule,
        String sql)
    {
        HepProgramBuilder programBuilder = new HepProgramBuilder();
        programBuilder.addRuleInstance(rule);
        final HepPlanner planner =
            new HepPlanner(programBuilder.createProgram());

        checkPlanning(
            preProgram,
            planner,
            sql);
    }

    /**
     * Checks the plan for a SQL statement before/after executing a given rule,
     * with a pre-program to prepare the tree.
     *
     * @param preProgram Program to execute before comparing before state
     * @param planner Planner
     * @param sql SQL query
     */
    protected void checkPlanning(
        HepProgram preProgram,
        RelOptPlanner planner,
        String sql)
    {
        final DiffRepository diffRepos = getDiffRepos();
        String sql2 = diffRepos.expand("sql", sql);
        RelNode relInitial = tester.convertSqlToRel(sql2);

        assertTrue(relInitial != null);

        ChainedRelMetadataProvider plannerChain =
            new ChainedRelMetadataProvider();
        DefaultRelMetadataProvider defaultProvider =
            new DefaultRelMetadataProvider();
        plannerChain.addProvider(defaultProvider);
        planner.registerMetadataProviders(plannerChain);
        relInitial.getCluster().setMetadataProvider(plannerChain);

        RelNode relBefore;
        if (preProgram == null) {
            relBefore = relInitial;
        } else {
            HepPlanner prePlanner = new HepPlanner(preProgram);
            prePlanner.setRoot(relInitial);
            relBefore = prePlanner.findBestExp();
        }

        assertTrue(relBefore != null);

        String planBefore = NL + RelOptUtil.toString(relBefore);
        diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

        planner.setRoot(relBefore);
        RelNode relAfter = planner.findBestExp();

        String planAfter = NL + RelOptUtil.toString(relAfter);
        diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    }

    /**
     * Creates a program which is a sequence of rules.
     *
     * @param rules Sequence of rules
     *
     * @return Program
     */
    protected static HepProgram createProgram(
        RelOptRule ... rules)
    {
        final HepProgramBuilder builder = new HepProgramBuilder();
        for (RelOptRule rule : rules) {
            builder.addRuleInstance(rule);
        }
        return builder.createProgram();
    }
}

// End RelOptTestBase.java
