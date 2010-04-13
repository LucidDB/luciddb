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

    protected RelOptSchema schema;
    protected RelDataType rowType;
    protected String name;

    //~ Constructors -----------------------------------------------------------

    protected RelOptAbstractTable(
        RelOptSchema schema,
        String name,
        RelDataType rowType)
    {
        this.schema = schema;
        this.name = name;
        this.rowType = rowType;
    }

    //~ Methods ----------------------------------------------------------------

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

    public void setRowType(RelDataType rowType)
    {
        this.rowType = rowType;
    }

    public RelOptSchema getRelOptSchema()
    {
        return schema;
    }

    public List<RelCollation> getCollationList()
    {
        return Collections.<RelCollation>emptyList();
    }
}

// End RelOptAbstractTable.java
