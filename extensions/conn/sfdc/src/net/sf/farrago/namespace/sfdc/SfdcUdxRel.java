/*
// $Id$
// SFDC Connector is an Eigenbase SQL/MED connector for Salesforce.com
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */
package net.sf.farrago.namespace.sfdc;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;


/**
 * SfdcUdxRel is an extension of {@link FarragoJavaUdxRel}
 *
 * @author Sunny Choi
 * @version $Id$
 */
class SfdcUdxRel
    extends FarragoJavaUdxRel
{
    //~ Instance fields --------------------------------------------------------

    // ~ Instance fields -------------------------------------------------------

    protected SfdcColumnSet table;
    protected FarragoUserDefinedRoutine udx;
    protected String serverMofId;

    //~ Constructors -----------------------------------------------------------

    // ~ Constructors ----------------------------------------------------------

    public SfdcUdxRel(
        RelOptCluster cluster,
        RexNode rexCall,
        RelDataType rowType,
        String serverMofId,
        SfdcColumnSet sfdcTable,
        FarragoUserDefinedRoutine udx)
    {
        super(cluster, rexCall, rowType, serverMofId, RelNode.emptyArray);
        this.table = sfdcTable;
        this.udx = udx;
        this.serverMofId = serverMofId;
    }

    //~ Methods ----------------------------------------------------------------

    // ~ Methods ---------------------------------------------------------------

    public SfdcColumnSet getTable()
    {
        return this.table;
    }

    public FarragoUserDefinedRoutine getUdx()
    {
        return this.udx;
    }

    public String getServerMofId()
    {
        return this.serverMofId;
    }
}

// End SfdcUdxRel.java
