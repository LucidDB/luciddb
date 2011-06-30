/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package org.eigenbase.relopt;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.Util;


/**
 * A <code>RelOptAbstractTable</code> is a partial implementation of {@link
 * RelOptTable}.
 *
 * @author jhyde
 * @version $Id$
 * @since May 3, 2002
 */
public abstract class RelOptAbstractTable
    implements RelOptTable
{
    //~ Instance fields --------------------------------------------------------

    protected final RelOptSchema schema;
    protected final RelDataType rowType;
    protected final List<RelDataTypeField> systemFieldList;
    protected final String name;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RelOptAbstractTable.
     *
     * @param schema Schema (may be null)
     * @param name Table name
     * @param rowType Type for rows stored in table (not including system
     *     fields)
     * @param systemFieldList List of system fields (may be empty, not null)
     */
    protected RelOptAbstractTable(
        RelOptSchema schema,
        String name,
        RelDataType rowType,
        List<RelDataTypeField> systemFieldList)
    {
        this.schema = schema;
        this.name = name;
        this.rowType = rowType;
        this.systemFieldList = systemFieldList;
        assert name != null;
        assert rowType != null;
        assert systemFieldList != null;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the name of this table.
     *
     * @return Table name
     */
    public String getName()
    {
        return name;
    }

    public String [] getQualifiedName()
    {
        return new String[] { name };
    }

    public double getRowCount()
    {
        return 100;
    }

    public RelDataType getRowType()
    {
        return rowType;
    }

    public RelOptSchema getRelOptSchema()
    {
        return schema;
    }

    public List<RelCollation> getCollationList()
    {
        return Collections.emptyList();
    }

    public List<RelDataTypeField> getSystemFieldList()
    {
        return systemFieldList;
    }
}

// End RelOptAbstractTable.java
