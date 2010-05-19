/*
// $Id$
// Farrago is an extensible data management system.
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
package net.sf.farrago.test;

import net.sf.farrago.query.*;
import net.sf.farrago.session.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.hep.*;


/**
 * FarragoTestPlanner provides an implementation of the {@link
 * FarragoSessionPlanner} interface which allows for precise control over the
 * heuristic planner used during testing.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoTestPlanner
    extends HepPlanner
    implements FarragoSessionPlanner
{
    //~ Instance fields --------------------------------------------------------

    private final FarragoPreparingStmt stmt;

    //~ Constructors -----------------------------------------------------------

    public FarragoTestPlanner(
        HepProgram program,
        FarragoPreparingStmt stmt)
    {
        super(program);
        this.stmt = stmt;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionPlanner
    public FarragoSessionPreparingStmt getPreparingStmt()
    {
        return stmt;
    }

    // implement FarragoSessionPlanner
    public void beginMedPluginRegistration(String serverClassName)
    {
        // don't care
    }

    // implement FarragoSessionPlanner
    public void endMedPluginRegistration()
    {
        // don't care
    }

    // implement RelOptPlanner
    public JavaRelImplementor getJavaRelImplementor(RelNode rel)
    {
        return stmt.getRelImplementor(
            rel.getCluster().getRexBuilder());
    }
}

// End FarragoTestPlanner.java
