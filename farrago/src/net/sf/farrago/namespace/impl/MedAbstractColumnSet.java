/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.namespace.impl;

import java.util.*;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * MedAbstractColumnSet is an abstract base class for implementations
 * of the {@link FarragoMedColumnSet} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractColumnSet extends RelOptAbstractTable
    implements FarragoQueryColumnSet
{
    //~ Instance fields -------------------------------------------------------

    private final String [] localName;
    private final String [] foreignName;
    private Properties tableProps;
    private Map columnPropMap;
    private FarragoPreparingStmt preparingStmt;
    private CwmNamedColumnSet cwmColumnSet;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new MedAbstractColumnSet.
     *
     * @param localName name of this ColumnSet as it will be known
     * within the Farrago system
     *
     * @param foreignName name of this ColumnSet as it is known
     * on the foreign server; may be null if no meaningful name
     * exists
     *
     * @param rowType row type descriptor
     */
    protected MedAbstractColumnSet(
        String [] localName,
        String [] foreignName,
        RelDataType rowType,
        Properties tableProps,
        Map columnPropMap)
    {
        super(null, localName[localName.length - 1], rowType);
        this.localName = localName;
        this.foreignName = foreignName;
        this.tableProps = tableProps;
        this.columnPropMap = columnPropMap;
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptTable
    public String [] getQualifiedName()
    {
        return localName;
    }

    /**
     * @return the name this ColumnSet is known by within
     * the Farrago system
     */
    public String [] getLocalName()
    {
        return localName;
    }

    /**
     * @return the name of this ColumnSet as it is known on the foreign server
     */
    public String [] getForeignName()
    {
        return foreignName;
    }

    /**
     * @return options specified by CREATE FOREIGN TABLE
     */
    public Properties getTableProperties()
    {
        return tableProps;
    }

    /**
     * @return map (from column name to Properties) of column options specified
     * by CREATE FOREIGN TABLE
     */
    public Map getColumnPropertyMap()
    {
        return columnPropMap;
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

    public boolean isMonotonic(String columnName)
    {
        return false;
    }
}


// End MedAbstractColumnSet.java
