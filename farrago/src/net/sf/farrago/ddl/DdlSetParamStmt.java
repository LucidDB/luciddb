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

import net.sf.farrago.session.*;

import org.eigenbase.sql.*;


/**
 * DdlSetParamStmt provides a common base class for DDL that alters
 * configuration values on repository objects.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public abstract class DdlSetParamStmt
    extends DdlStmt
{

    //~ Instance fields --------------------------------------------------------

    private final String paramName;
    private final SqlLiteral paramValue;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlSetParamStmt.
     *
     * @param paramName name of parameter to set
     * @param paramValue new value for parameter
     */
    public DdlSetParamStmt(
        String paramName,
        SqlLiteral paramValue)
    {
        super(null);
        this.paramName = paramName;
        this.paramValue = paramValue;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return name of the parameter set by this statement
     */
    public String getParamName()
    {
        return paramName;
    }

    /**
     * @return value of the parameter set by this statement
     */
    public SqlLiteral getParamValue()
    {
        return paramValue;
    }

    /**
     * Prevalidates an "ALTER ... SET "param" = 'value' statement. First
     * examines <code>primaryConfig</code> to see if the {@link #paramName} is a
     * valid name. If not and if <code>alternateConfig</code> is not null,
     * <code>alternateConfig</code> is tested. If either succeeds, {@link
     * #paraValue} is converted to the appropriate type and the setRefValue
     * mutator is called on the the RefObject.
     *
     * <p>Calls the following functions in the event of errors:
     *
     * <ul>
     * <li>{@link #handleInvalidName( FarragoSessionDdlValidator,
     * InvalideNameException)} - if <code>paramName</code> is not a member of
     * either RefObject.</li>
     * <li>{@link #handleReflectionException( FarragoSessionDdlValidator,
     * Exception)} - if there's an error converting {@link #paramValue}.</li>
     * <li>{@link #handleImmutableParam( FarragoSessionDdlValidator,
     * InvalidNameException)} - if it turns out the parameter is immutable.</li>
     * <li>{@link #handleTypeMismatch( FarragoSessionDdlValidator,
     * TypeMismatchException)} - if the <code>paramValue</code> is successfully
     * converted but does not match the expected type for the parameter.</li>
     * </ul>
     *
     * @param ddlValidator the DDL validator performing validation
     * @param primaryConfig the primary RefObject to check for param names
     * @param alternateConfig an alternate RefObject to use if <code>
     * primaryConfig</code> doesn't contain the parameter.
     */
    protected void preValidate(
        FarragoSessionDdlValidator ddlValidator,
        RefObject primaryConfig,
        RefObject alternateConfig)
    {
        Object oldValue;

        RefObject config = primaryConfig;
        try {
            oldValue = config.refGetValue(paramName);
        } catch (InvalidNameException ex) {
            if (alternateConfig == null) {
                handleInvalidName(ddlValidator, ex);
                return;
            } else {
                // Not in config, maybe in altConfig
                try {
                    oldValue = alternateConfig.refGetValue(paramName);

                    // if we get here, it's an altConfig parameter
                    config = alternateConfig;
                } catch (InvalidNameException ex2) {
                    handleInvalidName(ddlValidator, ex2);
                    return;
                }
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
                        new Class[] { String.class });
                newValue =
                    method.invoke(
                        null,
                        new Object[] { newValueAsString });
            } else {
                Constructor constructor =
                    oldValue.getClass().getConstructor(
                        new Class[] { String.class });
                newValue =
                    constructor.newInstance(new Object[] { newValueAsString });
            }
        } catch (Exception ex) {
            handleReflectionException(ddlValidator, ex);
            return;
        }

        try {
            config.refSetValue(paramName, newValue);
        } catch (InvalidNameException ex) {
            // We know the parameter exists, so InvalidNameException in this
            // context implies that it's immutable.
            handleImmutableParameter(ddlValidator, ex);
        } catch (TypeMismatchException ex) {
            handleTypeMismatch(ddlValidator, ex);
        }
    }

    /**
     * Handle invalide name exception. Called when {@link #paramName} is not
     * recognized as a member of the RefObject passed to {@link
     * #preValidate(FarragoSessionDdlValidator, RefObject, RefObject)}.
     *
     * @param ddlValidator the object passed to {@link #preValidate()}.
     * @param thrown the InvalidNameException generated
     */
    protected abstract void handleInvalidName(
        FarragoSessionDdlValidator ddlValidator,
        InvalidNameException thrown);

    /**
     * Handle reflection exception. Called when a reflection error occurs while
     * performing type conversion on {@link #paramValue}.
     *
     * @param ddlValidator the object passed to {@link #preValidate()}.
     * @param thrown the Exception
     */
    protected abstract void handleReflectionException(
        FarragoSessionDdlValidator ddlValidator,
        Exception thrown);

    /**
     * Handle immutable parameters. Called when {@link #paramName} is an
     * immutable parameter of the RefObject passed to {@link
     * #preValidate(FarragoSessionDdlValidator, RefObject, RefObject)}.
     *
     * @param ddlValidator the object passed to {@link #preValidate()}.
     * @param thrown the InvalidNameException generated (which in this case
     * indicates an immutable parameter)
     */
    protected abstract void handleImmutableParameter(
        FarragoSessionDdlValidator ddlValidator,
        InvalidNameException thrown);

    /**
     * Handle type mismatch. Called when {@link #paramValue} has successfully
     * undergone type conversion but is not the expected type.
     *
     * @param ddlValidator the object passed to {@link #preValidate()}.
     * @param thrown the TypeMismatchException thrown
     */
    protected abstract void handleTypeMismatch(
        FarragoSessionDdlValidator ddlValidator,
        TypeMismatchException thrown);
}

// End DdlSetParamStmt.java
