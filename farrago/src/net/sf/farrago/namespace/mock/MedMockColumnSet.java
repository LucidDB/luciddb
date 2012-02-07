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
package net.sf.farrago.namespace.mock;

import java.math.*;

import java.sql.*;

import java.util.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.jdbc.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * MedMockColumnSet provides a mock implementation of the {@link
 * FarragoMedColumnSet} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockColumnSet
    extends MedAbstractColumnSet
{
    //~ Instance fields --------------------------------------------------------

    final MedMockDataServer server;
    final long nRows;
    final String executorImpl;
    final String udxSpecificName;

    //~ Constructors -----------------------------------------------------------

    MedMockColumnSet(
        MedMockDataServer server,
        String [] localName,
        RelDataType rowType,
        long nRows,
        String executorImpl,
        String udxSpecificName)
    {
        super(localName, null, rowType, null, null);
        this.server = server;
        this.nRows = nRows;
        this.executorImpl = executorImpl;
        this.udxSpecificName = udxSpecificName;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptTable
    public double getRowCount()
    {
        return nRows;
    }

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        if (executorImpl.equals(MedMockDataServer.PROPVAL_FENNEL)) {
            // Use Fennel ExecStream.
            return new MedMockFennelRel(this, cluster, connection);
        }

        assert (executorImpl.equals(MedMockDataServer.PROPVAL_JAVA));

        // Example for how to post a warning
        long nRowsActual = nRows;
        if (nRowsActual < 0) {
            nRowsActual = 0;
            FarragoWarningQueue warningQueue =
                getPreparingStmt().getStmtValidator().getWarningQueue();
            warningQueue.postWarning(
                new SQLWarning("slow down:  mock turtle crossing"));
        }

        if (udxSpecificName == null) {
            // Use boring Java iterator.
            return new MedMockIterRel(this, cluster, connection);
        }

        // Otherwise, use the UDX supplied by the user.  All we have to do is
        // construct the arguments.
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode arg = rexBuilder.makeExactLiteral(new BigDecimal(nRows));

        // Call to super handles the rest.
        return toUdxRel(
            cluster,
            connection,
            udxSpecificName,
            server.getServerMofId(),
            new RexNode[] { arg });
    }
}

// End MedMockColumnSet.java
