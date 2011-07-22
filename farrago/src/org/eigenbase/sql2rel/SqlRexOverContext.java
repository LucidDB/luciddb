/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
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
package org.eigenbase.sql2rel;

import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlWindow;
/**
 * Class for holding window context while converting aggregate operations.
 *
 * @author jhyde
 * @version $Id$
 * @since 2005/8/3
 */
public class SqlRexOverContext
{
    private final SqlWindow window;
    private final RexNode [] orderKeys;
    private final RexNode [] partitionKeys;

    public SqlWindow getWindow()
    {
        return window;
    }

    public RexNode[] getOrderKeys()
    {
        return orderKeys;
    }

    public RexNode [] getPartitionKeys()
    {
        return partitionKeys;
    }

    public SqlRexOverContext(
        SqlWindow window, RexNode[] partitionKeys, RexNode[] orderKeys)
    {
        this.window = window;
        this.orderKeys = orderKeys;
        this.partitionKeys = partitionKeys;
    }
}
// End SqlRexOverContext.java