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
package net.sf.farrago.ddl;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.reltype.*;


/**
 * Statement for altering an identity column. This statement is one form of an
 * alter table statement and is possible because only one table action is
 * performed at a time.
 *
 * @author John Pham
 * @version $Id$
 */
public class DdlAlterIdentityColumnStmt
    extends DdlAlterStmt
{
    //~ Instance fields --------------------------------------------------------

    private FarragoSequenceOptions options;

    //~ Constructors -----------------------------------------------------------

    public DdlAlterIdentityColumnStmt(
        CwmColumn column,
        FarragoSequenceOptions options)
    {
        super(column);
        this.options = options;
    }

    //~ Methods ----------------------------------------------------------------

    protected void execute(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session)
    {
        FemAbstractTypedElement column =
            (FemAbstractTypedElement) getModelElement();
        FemSequenceGenerator sequence = null;
        if (column instanceof FemStoredColumn) {
            sequence = ((FemStoredColumn) column).getSequence();
        }
        if (sequence == null) {
            FarragoResource.instance().ValidatorAlterIdentityFailed.ex(
                column.getName());
        }
        RelDataType dataType =
            ddlValidator.getTypeFactory().createCwmElementType(column);

        FarragoSequenceAccessor accessor =
            session.getRepos().getSequenceAccessor(sequence.refMofId());
        accessor.alterSequence(options, dataType);
    }
}

// End DdlAlterIdentityColumnStmt.java
