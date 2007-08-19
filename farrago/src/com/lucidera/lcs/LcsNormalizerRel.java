/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * A relation which expands bitmap tuples of the form [keys, bitmaps] into
 * repetitious tuples of the form [keys]. One tuple will be output for each bit
 * set in an input bitmap.
 *
 * @author John Pham
 * @version $Id$
 */
public class LcsNormalizerRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    private final FarragoRepos repos;

    //~ Constructors -----------------------------------------------------------

    public LcsNormalizerRel(
        RelOptCluster cluster,
        RelNode child)
    {
        super(cluster, child);
        repos = FennelRelUtil.getRepos(this);
    }

    //~ Methods ----------------------------------------------------------------

    // implement AbstractRelNode
    public LcsNormalizerRel clone()
    {
        LcsNormalizerRel clone =
            new LcsNormalizerRel(
                getCluster(),
                getChild());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement AbstractRelNode
    protected RelDataType deriveRowType()
    {
        RelDataType childType = getChild().getRowType();
        List<RelDataTypeField> childFields = childType.getFieldList();
        final int nKeys = childFields.size() - 3;
        final List<RelDataTypeField> keyFields = childFields.subList(0, nKeys);

        return getCluster().getTypeFactory().createStructType(
            new RelDataTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return nKeys;
                }

                public String getFieldName(int index)
                {
                    return keyFields.get(index).getName();
                }

                public RelDataType getFieldType(int index)
                {
                    return keyFields.get(index).getType();
                }
            });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemLbmNormalizerStreamDef normalizer =
            repos.newFemLbmNormalizerStreamDef();
        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild(), 0),
            normalizer);

        return normalizer;
    }
}

// End LcsNormalizerRel.java
