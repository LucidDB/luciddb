/*
// $Id$
// Firewater is a scaleout column store DBMS.
// Copyright (C) 2009-2009 John V. Sichi
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
package net.sf.firewater;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

/**
 * FirewaterReplicatedTableRel represents a replicated table in
 * a query plan before we have chosen which replica to access
 * (after which it is typically replaced by a JDBC query).
 *
 * @author John Sichi
 * @version $Id$
 */
public class FirewaterReplicatedTableRel extends TableAccessRelBase
{
    /**
     * Refinement for super.table.
     */
    final FirewaterColumnSet replicatedTable;

    /**
     * Creates a new FirewaterReplicatedTableRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param replicatedTable table being accessed
     * @param connection connection
     */
    public FirewaterReplicatedTableRel(
        RelOptCluster cluster,
        FirewaterColumnSet replicatedTable,
        RelOptConnection connection)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            replicatedTable,
            connection);
        this.replicatedTable = replicatedTable;
    }
}

// End FirewaterReplicatedTableRel.java
