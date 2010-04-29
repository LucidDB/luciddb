/*
// $Id$
// SFDC Connector is a SQL/MED connector for Salesforce.com for Farrago
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
