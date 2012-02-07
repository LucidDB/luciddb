/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
