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

import net.sf.farrago.defimpl.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


/**
 * DdlSetSessionParamStmt represents the ALTER SESSION SET ... statement.
 *
 * @author John Pham
 * @version $Id$
 */
public class DdlSetSessionParamStmt
    extends DdlStmt
{
    //~ Instance fields --------------------------------------------------------

    final private String paramName;
    final private SqlLiteral paramValue;
    private FemLabel labelParamValue;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlSetSessionParamStmt.
     *
     * @param paramName name of parameter to set
     * @param paramValue new value for parameter
     */
    public DdlSetSessionParamStmt(
        String paramName,
        SqlLiteral paramValue)
    {
        super(null);
        this.paramName = paramName;
        this.paramValue = paramValue;
        this.labelParamValue = null;
    }

    //~ Methods ----------------------------------------------------------------

    // override DdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        FarragoSession session = ddlValidator.getInvokingSession();
        String valueString = paramValue.toValue();

        session.getPersonality().validateSessionVariable(
            ddlValidator,
            session.getSessionVariables(),
            paramName,
            valueString);

        // Retrieve the underlying label object
        if (paramName.equals(FarragoDefaultSessionPersonality.LABEL)) {
            // Labels can't be set inside UDR's because UDR's currently
            // run as a single transaction initiated by the caller of the
            // UDR.  So setting the label will be a  no-op.
            if (FarragoUdrRuntime.inUdr()) {
                throw FarragoResource.instance().ValidatorSetLabelInUdr.ex();
            }
            if (valueString != null) {
                SqlIdentifier unQualifiedName =
                    new SqlIdentifier(
                        valueString,
                        new SqlParserPos(0, 0));
                labelParamValue =
                    ddlValidator.getStmtValidator().findUnqualifiedObject(
                        unQualifiedName,
                        FemLabel.class);
            }
        }
    }

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    /**
     * @return the name of the parameter being set
     */
    public String getParamName()
    {
        return paramName;
    }

    /**
     * @return the value of the parameter if the label parameter is being set
     */
    public FemLabel getLabelParamValue()
    {
        return labelParamValue;
    }
}

// End DdlSetSessionParamStmt.java
