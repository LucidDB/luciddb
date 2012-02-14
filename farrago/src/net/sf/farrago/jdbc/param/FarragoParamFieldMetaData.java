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

import java.io.*;

import java.sql.*;


/**
 * This defines the per parameter field metadata required by the client-side
 * driver to implement the JDBC ParameterMetaData API. This class is JDK 1.4
 * compatible.
 *
 * @author Angel Chang
 * @version $Id$
 * @see net.sf.farrago.jdbc.engine.FarragoParamFieldMetaDataFactory
 */
public class FarragoParamFieldMetaData
    implements Serializable
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * SerialVersionUID created with JDK 1.5 serialver tool.
     */
    private static final long serialVersionUID = 5042520840301805755L;

    //~ Instance fields --------------------------------------------------------

    /**
     * SQL paramMetaData of this field.
     */
    public int type;

    /**
     * SQL className.
     */
    public String className;

    /**
     * SQL typename of this field.
     */
    public String typeName;

    /**
     * precision of this field.
     */
    public int precision;

    /**
     * scale of this field.
     */
    public int scale;

    /**
     * indicates whether this parameter field is nullable. One of {{@link
     * java.sql.ParameterMetaData#parameterNoNulls}, {@link
     * java.sql.ParameterMetaData#parameterNullable}, {@link
     * java.sql.ParameterMetaData#parameterNullableUnknown}}.
     */
    public int nullable = ParameterMetaData.parameterNullableUnknown;

    /**
     * indicates whether this field is signed.
     */
    public boolean signed;

    /**
     * indicate the parameter mode. One of {{@link
     * java.sql.ParameterMetaData#parameterModeUnknown}, {@link
     * java.sql.ParameterMetaData#parameterModeIn}, {@link
     * java.sql.ParameterMetaData#parameterModeOut}, {@link
     * java.sql.ParameterMetaData#parameterModeInOut}.
     */
    public int mode = ParameterMetaData.parameterModeUnknown;

    /**
     * String describing the parameter type Usually of the form: typeName
     * (precision, scale)
     */
    public String paramTypeStr;
}

// End FarragoParamFieldMetaData.java
