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

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rel.*;

/**
 * An abstract base for implementations of RelOptTable which access data
 * described by Farrago's catalog.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoQueryNamedColumnSet
    extends RelOptAbstractTable
    implements FarragoQueryColumnSet
{
    //~ Instance fields -------------------------------------------------------

    /** Catalog definition of column set. */
    private CwmNamedColumnSet cwmColumnSet;

    /** Refinement for RelOptAbstractTable.schema. */
    private FarragoPreparingStmt preparingStmt;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoQueryNamedColumnSet object.
     *
     * @param cwmTable catalog definition for column set
     *
     * @param rowType type for rows stored in column set
     */
    FarragoQueryNamedColumnSet(
        CwmNamedColumnSet cwmColumnSet,
        RelDataType rowType)
    {
        super(null,cwmColumnSet.getName(),rowType);
        this.cwmColumnSet = cwmColumnSet;
    }

    //~ Methods ---------------------------------------------------------------

    // override RelOptAbstractTable
    public String [] getQualifiedName()
    {
        return new String [] 
            {
                cwmColumnSet.getNamespace().getNamespace().getName(),
                cwmColumnSet.getNamespace().getName(),
                cwmColumnSet.getName()
            };
    }

    // implement FarragoQueryColumnSet
    public FarragoPreparingStmt getPreparingStmt()
    {
        return preparingStmt;
    }
    
    // implement FarragoQueryColumnSet
    public void setPreparingStmt(FarragoPreparingStmt stmt)
    {
        preparingStmt = stmt;
    }
    
    // implement FarragoQueryColumnSet
    public void setCwmColumnSet(CwmNamedColumnSet cwmColumnSet)
    {
        this.cwmColumnSet = cwmColumnSet;
    }

    // implement FarragoQueryColumnSet
    public CwmNamedColumnSet getCwmColumnSet()
    {
        return cwmColumnSet;
    }
}


// End FarragoTable.java
