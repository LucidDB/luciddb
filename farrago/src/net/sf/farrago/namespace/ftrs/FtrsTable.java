/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.namespace.ftrs;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * An implementation of RelOptTable for accessing data stored in FTRS.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsTable
    extends MedAbstractColumnSet
{

    //~ Instance fields --------------------------------------------------------

    private FtrsIndexGuide indexGuide;

    //~ Constructors -----------------------------------------------------------

    FtrsTable(
        String [] localName,
        RelDataType rowType,
        Properties tableProps,
        Map columnPropMap)
    {
        super(localName, null, rowType, tableProps, columnPropMap);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        return
            new FtrsIndexScanRel(
                cluster,
                this,
                FarragoCatalogUtil.getClusteredIndex(
                    getPreparingStmt().getRepos(),
                    getCwmColumnSet()),
                connection,
                null,
                false);
    }

    public FtrsIndexGuide getIndexGuide()
    {
        // have to defer initialization because not all information
        // is available at construction time
        if (indexGuide == null) {
            indexGuide =
                new FtrsIndexGuide(
                    getPreparingStmt().getFarragoTypeFactory(),
                    getCwmColumnSet());
        }
        return indexGuide;
    }
}

// End FtrsTable.java
