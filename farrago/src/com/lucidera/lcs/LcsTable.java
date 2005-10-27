/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;
import net.sf.farrago.catalog.*;

import org.eigenbase.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * An implementation of RelOptTable for accessing data in a LucidDB
 * column-store.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class LcsTable extends MedAbstractColumnSet
{
    //~ Constructors ----------------------------------------------------------

    LcsTable(
        String [] localName,
        RelDataType rowType,
        Properties tableProps,
        Map columnPropMap)
    {
        super(localName, null, rowType, tableProps, columnPropMap);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        // TODO jvs 26-Oct-2005
        throw Util.needToImplement(this);
    }
}


// End LcsTable.java
