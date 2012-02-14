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
package net.sf.farrago.type;

import java.sql.*;
import java.util.*;

import org.eigenbase.reltype.*;


/**
 * FarragoParameterMetaData implements the ParameterMetaData interface by
 * reading a Farrago type descriptor.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoParameterMetaData
    extends FarragoJdbcMetaDataImpl
    implements ParameterMetaData
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoParameterMetaData object.
     *
     * @param rowType type info to return
     */
    public FarragoParameterMetaData(RelDataType rowType)
    {
        super(
            rowType,
            Collections.<List<String>>nCopies(rowType.getFieldCount(), null));
    }

    //~ Methods ----------------------------------------------------------------

    // implement ParameterMetaData
    public String getParameterClassName(int param)
        throws SQLException
    {
        return getFieldClassName(param);
    }

    // implement ParameterMetaData
    public int getParameterCount()
        throws SQLException
    {
        return getFieldCount();
    }

    // implement ParameterMetaData
    public int getParameterMode(int param)
        throws SQLException
    {
        return ParameterMetaData.parameterModeIn;
    }

    // implement ParameterMetaData
    public int getParameterType(int param)
        throws SQLException
    {
        return getFieldJdbcType(param);
    }

    // implement ParameterMetaData
    public String getParameterTypeName(int param)
        throws SQLException
    {
        return getFieldTypeName(param);
    }

    // implement ParameterMetaData
    public int getPrecision(int param)
        throws SQLException
    {
        return getFieldPrecision(param);
    }

    // implement ParameterMetaData
    public int getScale(int param)
        throws SQLException
    {
        return getFieldScale(param);
    }

    // implement ParameterMetaData
    public int isNullable(int param)
        throws SQLException
    {
        return isFieldNullable(param);
    }

    // implement ParameterMetaData
    public boolean isSigned(int param)
        throws SQLException
    {
        return isFieldSigned(param);
    }
}

// End FarragoParameterMetaData.java
