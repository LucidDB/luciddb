/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2003-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package net.sf.farrago.query;

import net.sf.farrago.session.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * Static utilities for Farrago RelNode implementations.
 *
 * @author John Pham
 * @version $Id$
 */
public abstract class FarragoRelUtil
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return the preparing stmt that a relational expression belongs to
     */
    public static FarragoPreparingStmt getPreparingStmt(RelNode rel)
    {
        RelOptCluster cluster = rel.getCluster();
        RelOptPlanner planner = cluster.getPlanner();
        if (planner instanceof FarragoSessionPlanner) {
            FarragoSessionPlanner farragoPlanner =
                (FarragoSessionPlanner) planner;
            return (FarragoPreparingStmt) farragoPlanner.getPreparingStmt();
        } else {
            return null;
        }
    }
}

// End FennelRelUtil.java
