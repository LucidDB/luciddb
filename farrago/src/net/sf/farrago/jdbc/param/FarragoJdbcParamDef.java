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
package net.sf.farrago.jdbc.param;

import java.math.*;

import java.sql.*;

import java.util.*;

import org.eigenbase.util.*;


/**
 * Handles data conversion for a dynamic parameter (refactored from
 * FarragoJdbcEngineParamDef) Enforces constraints on parameters. The
 * constraints are:
 *
 * <ol>
 * <li>Ensures that null values cannot be inserted into not-null columns.
 * <li>Ensures that value is the right paramMetaData.
 * <li>Ensures that the value is within range. For example, you can't insert a
 * 10001 into a DECIMAL(5) column.
 * </ol>
 *
 * <p>TODO: Actually enfore these constraints. This class is JDK 1.4 compatible.
 *
 * @author Angel Chang
 * @version $Id$
 */
public class FarragoJdbcParamDef
{
    //~ Instance fields --------------------------------------------------------

    final FarragoParamFieldMetaData paramMetaData;
    final String paramName;

    //~ Constructors -----------------------------------------------------------

    FarragoJdbcParamDef(
        String paramName,
        FarragoParamFieldMetaData paramMetaData)
    {
        this.paramName = paramName;
        this.paramMetaData = paramMetaData;
    }

    //~ Methods ----------------------------------------------------------------

    public String getParamName()
    {
        return paramName;
    }

    public FarragoParamFieldMetaData getParamMetaData()
    {
        return paramMetaData;
    }

    public Object scrubValue(Object x)
    {
        if (x == null) {
            checkNullable();
        }
        return x;
    }

    public Object scrubValue(Object x, Calendar cal)
    {
        return scrubValue(x);
            //throw new UnsupportedOperationException();
    }

    protected void checkNullable()
    {
        if (paramMetaData.nullable == ParameterMetaData.parameterNoNulls) {
            throw newNotNullable();
        }
    }

    protected void checkRange(
        BigInteger value,
        BigInteger min,
        BigInteger max)
    {
        if ((value.compareTo(min) < 0) || (value.compareTo(max) > 0)) {
            throw newValueOutOfRange(value);
        }
    }

    protected void checkRange(long value, long min, long max)
    {
        if ((value < min) || (value > max)) {
            // For JDK 1.4 compatibility
            throw newValueOutOfRange(new Long(value));
            //throw newValueOutOfRange(Long.valueOf(value));
        }
    }

    protected void checkRange(double value, double min, double max)
    {
        if ((value < min) || (value > max)) {
            // For JDK 1.4 compatibility
            throw newValueOutOfRange(new Double(value));
            //throw newValueOutOfRange(Double.valueOf(value));
        }
    }

    /**
     * Returns an error that the value is not valid for the desired SQL type.
     */
    protected EigenbaseException newInvalidType(Object x)
    {
        // TODO: Change to use client resources
        return new EigenbaseException(
            "Cannot assign a value of Java class " + x
            .getClass().getName()
            + " to parameter of type " + paramMetaData.paramTypeStr,
            null);

        //return FarragoResource.instance().ParameterValueIncompatible.ex(
        //    x.getClass().getName(),
        //    paramMetaData.paramTypeStr);
    }

    /**
     * Returns an error that the value is not nullable
     */
    protected EigenbaseException newNotNullable()
    {
        // TODO: Change to use client resources
        return new EigenbaseException(
            "Cannot assign NULL to parameter '" + paramName + "' of type "
            + paramMetaData.paramTypeStr + " NOT NULL",
            null);

        //return FarragoResource.instance().ParameterValueNotNullable.ex(
        //    paramMetaData.paramTypeStr);
    }

    /**
     * Returns an error that the value cannot be converted to the desired SQL
     * type.
     */
    protected EigenbaseException newInvalidFormat(Object x)
    {
        // TODO: Change to use client resources
        return new EigenbaseException(
            "Value '" + x + "' cannot be converted to parameter of type "
            + paramMetaData.paramTypeStr,
            null);

        //return FarragoResource.instance().ParameterValueInvalidFormat.ex(
        //    x.toString(),
        //    paramMetaData.paramTypeStr);
    }

    /**
     * Returns an error the value is too long to be converted to the desired SQL
     * type.
     */
    protected EigenbaseException newValueTooLong(Object x)
    {
        // TODO: Change to use client resources
        return new EigenbaseException(
            "Value '" + x + "' is too long for parameter of type "
            + paramMetaData.paramTypeStr,
            null);

        //return FarragoResource.instance().ParameterValueTooLong.ex(
        //    x.toString(),
        //    paramMetaData.paramTypeStr);
    }

    /**
     * Returns an error the value is out of range and cannot be converted to the
     * desired SQL type.
     */
    protected EigenbaseException newValueOutOfRange(Object x)
    {
        // TODO: Change to use client resources
        return new EigenbaseException(
            "Value '" + x + "' is out of range for parameter of type "
            + paramMetaData.paramTypeStr,
            null);

        //return FarragoResource.instance().ParameterValueOutOfRange.ex(
        //    x.toString(),
        //    paramMetaData.paramTypeStr);
    }
}

// End FarragoJdbcParamDef.java
