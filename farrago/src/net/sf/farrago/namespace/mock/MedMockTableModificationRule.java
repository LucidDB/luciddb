/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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
package net.sf.farrago.namespace.mock;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;


/**
 * MockTableModificationRule is a rule for converting an abstract {@link
 * TableModificationRel} into a corresponding local mock table update (always
 * returning rowcount 0, since local mock tables never store any data).
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockTableModificationRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new MockTableModificationRule object.
     */
    public MedMockTableModificationRule()
    {
        super(new RelOptRuleOperand(
                TableModificationRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(RelNode.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return CallingConvention.ITERATOR;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        TableModificationRel tableModification =
            (TableModificationRel) call.rels[0];

        // TODO jvs 13-Sept-2004:  disallow updates to mock foreign tables
        if (!(tableModification.getTable() instanceof MedMockColumnSet)) {
            return;
        }

        MedMockColumnSet targetColumnSet = (MedMockColumnSet)
            tableModification.getTable();

        // create a 1-row column set with the correct type for rowcount;
        // single value returned will be 0, which is what we want
        MedMockColumnSet rowCountColumnSet =
            new MedMockColumnSet(
                targetColumnSet.getLocalName(),
                tableModification.getRowType(),
                1,
                targetColumnSet.executorImpl);
        
        call.transformTo(
            rowCountColumnSet.toRel(
                tableModification.getCluster(),
                tableModification.getConnection()));
    }
}


// End MedMockTableModificationRule.java
