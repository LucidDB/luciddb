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

package net.sf.farrago.query;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.namespace.*;

import net.sf.saffron.core.*;
import net.sf.saffron.ext.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;

/**
 * An abstract base for implementations of SaffronTable which access data
 * described by Farrago's catalog.
 *
 * @author John V. Sichi
 * @version $Id$
 */
abstract class FarragoTable
    extends AbstractTable
    implements FarragoMedColumnSet
{
    //~ Instance fields -------------------------------------------------------

    /** Catalog definition of table. */
    protected final CwmNamedColumnSet cwmTable;

    /** Refinement for AbstractTable.schema. */
    protected final FarragoPreparingStmt preparingStmt;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoTable object.
     *
     * @param preparingStmt statement through which this table is being accessed
     * @param cwmTable catalog definition for table
     * @param rowType type for rows stored in table
     */
    FarragoTable(
        FarragoPreparingStmt preparingStmt,
        CwmNamedColumnSet cwmTable,
        SaffronType rowType)
    {
        super(preparingStmt,cwmTable.getName(),rowType);
        this.preparingStmt = preparingStmt;
        this.cwmTable = cwmTable;
    }

    //~ Methods ---------------------------------------------------------------

    // override AbstractTable
    public String [] getQualifiedName()
    {
        return new String [] 
            {
                cwmTable.getNamespace().getNamespace().getName(),
                cwmTable.getNamespace().getName(),
                cwmTable.getName() };
    }
}


// End FarragoTable.java
