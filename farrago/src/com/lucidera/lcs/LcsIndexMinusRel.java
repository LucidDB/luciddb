/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.lcs;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;


/**
 * LcsIndexMinusRel is a relation for "subtracting" the 2nd through Nth inputs
 * from the first input. The input to this relation must be more than one.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class LcsIndexMinusRel
    extends LcsIndexBitOpRel
{

    //~ Constructors -----------------------------------------------------------

    public LcsIndexMinusRel(
        RelOptCluster cluster,
        RelNode [] inputs,
        LcsTable lcsTable,
        FennelRelParamId startRidParamId,
        FennelRelParamId rowLimitParamId)
    {
        super(cluster, inputs, lcsTable, startRidParamId, rowLimitParamId);
    }

    //~ Methods ----------------------------------------------------------------

    public Object clone()
    {
        return
            new LcsIndexMinusRel(
                getCluster(),
                RelOptUtil.clone(getInputs()),
                lcsTable,
                startRidParamId,
                rowLimitParamId);
    }

    // implement RelNode
    public double getRows()
    {
        // get the number of rows from the first child and then reduce it
        // by the number of children
        double anchorRows = RelMetadataQuery.getRowCount(inputs[0]);
        return anchorRows / inputs.length;
    }

    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemLbmMinusStreamDef minusStream =
            lcsTable.getIndexGuide().newBitmapMinus(
                implementor.translateParamId(startRidParamId),
                implementor.translateParamId(rowLimitParamId),
                inputs[0]);

        setBitOpChildStreams(implementor, minusStream);

        return minusStream;
    }
}

//End LcsIndexMinusRel.java
