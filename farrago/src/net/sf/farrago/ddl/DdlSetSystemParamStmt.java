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

import java.lang.reflect.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;


/**
 * DdlSetSystemParamStmt represents the ALTER SYSTEM SET ... statement.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlSetSystemParamStmt
    extends DdlSetParamStmt
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlSetSystemParamStmt.
     *
     * @param paramName name of parameter to set
     * @param paramValue new value for parameter
     */
    public DdlSetSystemParamStmt(
        String paramName,
        SqlLiteral paramValue)
    {
        super(paramName, paramValue);
    }

    //~ Methods ----------------------------------------------------------------

    // override DdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        super.preValidate(ddlValidator);

        FemFarragoConfig farragoConfig =
            ddlValidator.getRepos().getCurrentConfig();

        preValidate(
            ddlValidator,
            farragoConfig,
            farragoConfig.getFennelConfig());
    }

    // implement DdlSetParamStmt
    protected void handleInvalidName(
        FarragoSessionDdlValidator ddlValidator,
        InvalidNameException thrown)
    {
        throw FarragoResource.instance().ValidatorUnknownSysParam.ex(
            ddlValidator.getRepos().getLocalizedObjectName(getParamName()));
    }

    // implement DdlSetParamStmt
    protected void handleReflectionException(
        FarragoSessionDdlValidator ddlValidator,
        Exception thrown)
    {
        throw FarragoResource.instance().ValidatorSysParamTypeMismatch.ex(
            getParamValue().toString(),
            ddlValidator.getRepos().getLocalizedObjectName(getParamName()));
    }

    // implement DdlSetParamStmt
    protected void handleImmutableParameter(
        FarragoSessionDdlValidator ddlValidator,
        InvalidNameException thrown)
    {
        throw FarragoResource.instance().ValidatorImmutableSysParam.ex(
            ddlValidator.getRepos().getLocalizedObjectName(getParamName()));
    }

    // implement DdlSetParamStmt
    protected void handleTypeMismatch(
        FarragoSessionDdlValidator ddlValidator,
        TypeMismatchException thrown)
    {
        throw FarragoResource.instance().ValidatorSysParamTypeMismatch.ex(
            getParamValue().toString(),
            getParamName());
    }

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }
}

// End DdlSetSystemParamStmt.java
