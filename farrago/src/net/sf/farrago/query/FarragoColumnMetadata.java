/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
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

import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.namespace.impl.*;

import org.eigenbase.rel.*;


/**
 * FarragoColumnMetadata is a default Farrago implementation of
 * MedAbstractColumnMetadata for table level RelNodes. Note that it does not
 * account for projection or UDTs.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FarragoColumnMetadata
    extends MedAbstractColumnMetadata
{
    //~ Methods ----------------------------------------------------------------

    protected int mapColumnToField(
        RelNode rel,
        FemAbstractColumn keyCol)
    {
        if (keyCol.getOrdinal() >= numColumns(rel)) {
            return -1;
        }
        return keyCol.getOrdinal();
    }

    protected int mapFieldToColumnOrdinal(RelNode rel, int fieldNo)
    {
        if ((fieldNo == -1) || (fieldNo >= numColumns(rel))) {
            return -1;
        } else {
            return fieldNo;
        }
    }

    protected FemAbstractColumn mapFieldToColumn(RelNode rel, int fieldNo)
    {
        int colno = mapFieldToColumnOrdinal(rel, fieldNo);
        if ((colno == -1) || (colno >= numColumns(rel))) {
            return null;
        } else {
            return (FemAbstractColumn) ((MedAbstractColumnSet) rel.getTable())
                .getCwmColumnSet().getFeature().get(colno);
        }
    }

    private int numColumns(RelNode rel)
    {
        return ((MedAbstractColumnSet) rel.getTable()).getCwmColumnSet()
            .getFeature().size();
    }
}

// End FarragoColumnMetadata.java
