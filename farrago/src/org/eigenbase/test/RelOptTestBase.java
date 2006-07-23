/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

    protected void checkPlanning(
        HepProgram program,
        String sql)
    {
        checkPlanning(
            new HepPlanner(program),
            sql);
    }

    protected void checkPlanning(
        RelOptPlanner planner,
        String sql)
    {
        final DiffRepository diffRepos = getDiffRepos();
        String sql2 = diffRepos.expand("sql", sql);
        RelNode relBefore = tester.convertSqlToRel(sql2);

        assertTrue(relBefore != null);

        String planBefore = NL + RelOptUtil.toString(relBefore);
        diffRepos.assertEquals("planBefore", "${planBefore}", planBefore);

        planner.setRoot(relBefore);
        RelNode relAfter = planner.findBestExp();

        String planAfter = NL + RelOptUtil.toString(relAfter);
        diffRepos.assertEquals("planAfter", "${planAfter}", planAfter);
    }
}

// End RelOptTestBase.java
