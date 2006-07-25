/*
// $Id$
// Farrago is an extensible data management system.
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
package net.sf.farrago.query;

import java.util.List;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;

import openjava.ptree.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * FennelValuesRel is Fennel implementation of {@link ValuesRel}. It corresponds
 * to fennel::ValuesExecStream via {@link ValuesStreamDef}, and guarantees the
 * order of the tuples it produces, making it usable for such purposes as the
 * search input to an index scan, where order may matter for both performance
 * and correctness.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelValuesRel
    extends ValuesRelBase
    implements FennelRel
{

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelValuesRel. Note that tuples passed in become owned by
     * this rel (without a deep copy), so caller must not modify them after this
     * call, otherwise bad things will happen.
     *
     * @param cluster .
     * @param rowType row type for tuples produced by this rel
     * @param tuples 2-dimensional array of tuple values to be produced; outer
     * list contains tuples; each inner list is one tuple; all tuples must be of
     * same length, conforming to rowType
     */
    public FennelValuesRel(
        RelOptCluster cluster,
        RelDataType rowType,
        List<List<RexLiteral>> tuples)
    {
        super(
            cluster,
            rowType,
            tuples,
            new RelTraitSet(FENNEL_EXEC_CONVENTION));
    }

    //~ Methods ----------------------------------------------------------------

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemValuesStreamDef streamDef = repos.newFemValuesStreamDef();

        streamDef.setTupleBytesBase64(
            FennelRelUtil.convertTuplesToBase64String(rowType, tuples));

        return streamDef;
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return Literal.constantNull();
    }

    public RelFieldCollation [] getCollations()
    {
        // TODO:  if tuples.size() == 1, say it's trivially sorted
        return RelFieldCollation.emptyCollationArray;
    }
}

// End FennelValuesRel.java
