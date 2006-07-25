/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

// End DdlSetSystemParam.java
