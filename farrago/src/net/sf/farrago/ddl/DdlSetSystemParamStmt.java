/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
public class DdlSetSystemParamStmt extends DdlStmt
{
    //~ Instance fields -------------------------------------------------------

    private final String paramName;
    private final SqlLiteral paramValue;

    //~ Constructors ----------------------------------------------------------

    /**
     * Construct a new DdlSetSystemParamStmt.
     *
     * @param paramName name of parameter to set
     *
     * @param paramValue new value for parameter
     */
    public DdlSetSystemParamStmt(
        String paramName,
        SqlLiteral paramValue)
    {
        super(null);
        this.paramName = paramName;
        this.paramValue = paramValue;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * .
     *
     * @return name of the parameter set by this statement
     */
    public String getParamName()
    {
        return paramName;
    }

    // override DdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        super.preValidate(ddlValidator);

        FemFarragoConfig farragoConfig =
            ddlValidator.getRepos().getCurrentConfig();
        RefObject config = farragoConfig;

        Object oldValue;

        try {
            oldValue = config.refGetValue(paramName);
        } catch (InvalidNameException ex) {
            // not a Farrago param, but maybe a Fennel param
            try {
                oldValue =
                    farragoConfig.getFennelConfig().refGetValue(paramName);

                // if we get here, it's a Fennel parameter
                config = farragoConfig.getFennelConfig();
            } catch (InvalidNameException ex2) {
                throw FarragoResource.instance().newValidatorUnknownSysParam(
                    ddlValidator.getRepos().getLocalizedObjectName(paramName));
            }
        }

        String newValueAsString = paramValue.toValue();

        // TODO:  use a generic type conversion facility.  Also, this assumes
        // parameters are never optional.
        Object newValue;
        try {
            if (oldValue instanceof RefEnum) {
                Method method =
                    oldValue.getClass().getMethod(
                        "forName",
                        new Class [] { String.class });
                newValue =
                    method.invoke(
                        null,
                        new Object [] { newValueAsString });
            } else {
                Constructor constructor =
                    oldValue.getClass().getConstructor(
                        new Class [] { String.class });
                newValue =
                    constructor.newInstance(
                        new Object [] { newValueAsString });
            }
        } catch (Exception ex) {
            throw FarragoResource.instance().newValidatorSysParamTypeMismatch(
                paramValue.toString(),
                ddlValidator.getRepos().getLocalizedObjectName(paramName));
        }

        try {
            config.refSetValue(paramName, newValue);
        } catch (InvalidNameException ex) {
            // We know the parameter exists, so InvalidNameException in this
            // context implies that it's immutable.
            throw FarragoResource.instance().newValidatorImmutableSysParam(
                ddlValidator.getRepos().getLocalizedObjectName(paramName));
        } catch (TypeMismatchException ex) {
            throw FarragoResource.instance().newValidatorSysParamTypeMismatch(
                paramValue.toString(),
                paramName);
        }
    }

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }
}


// End DdlSetSystemParam.java
