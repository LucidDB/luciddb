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
package net.sf.farrago.namespace.ftrs;

import java.util.*;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;
import net.sf.farrago.catalog.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * An implementation of RelOptTable for accessing data stored in FTRS.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsTable extends MedAbstractColumnSet
{
    //~ Constructors ----------------------------------------------------------

    FtrsTable(
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
        return new FtrsIndexScanRel(
            cluster,
            this,
            FarragoCatalogUtil.getClusteredIndex(
                getPreparingStmt().getRepos(),
                getCwmColumnSet()),
            connection,
            null,
            false);
    }
}


// End FtrsTable.java
