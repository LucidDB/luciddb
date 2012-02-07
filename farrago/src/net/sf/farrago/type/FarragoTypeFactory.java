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

import javax.jmi.model.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;

import openjava.ptree.*;

import org.eigenbase.oj.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;


/**
 * FarragoTypeFactory is a Farrago-specific refinement of the RelDataTypeFactory
 * interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoTypeFactory
    extends OJTypeFactory
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return associated FarragoRepos
     */
    public FarragoRepos getRepos();

    /**
     * Creates a type which represents the datatype of a
     * FemAbstractTypedElement.
     *
     * @param element CWM typed element
     *
     * @return generated type
     */
    public RelDataType createCwmElementType(
        FemAbstractTypedElement element);

    /**
     * Creates a type which represents a CwmSqldataType.
     *
     * @param cwmType CWM type instance
     *
     * @return generated type
     */
    public RelDataType createCwmType(
        CwmSqldataType cwmType);

    /**
     * Creates a type which represents a structured row based on a classifier
     * definition from the catalog.
     *
     * @param classifier definition of classifier
     *
     * @return generated type, or null if classifier had no features
     */
    public RelDataType createStructTypeFromClassifier(CwmClassifier classifier);

    /**
     * Creates a type which represents the row datatype of a JDBC ResultSet.
     * Optionally, unsupported types can be replaced with substitutes. In the
     * worst case, the substitute is VARCHAR(1024). Less drastic examples are
     * ignoring datetime fractional seconds precision or capping numeric
     * precision at our maximum.
     *
     * @param metaData metadata for JDBC ResultSet
     * @param substitute if true, use substitutions; if false, throw exception
     * for unsupported types or type attributes
     *
     * @return generated type
     */
    public RelDataType createResultSetType(
        ResultSetMetaData metaData,
        boolean substitute);

    /**
     * Creates a type which represents the row datatype of a JDBC ResultSet.
     * Optionally, unsupported types can be replaced with substitutes. In the
     * worst case, the substitute is VARCHAR(1024). Less drastic examples are
     * ignoring datetime fractional seconds precision or capping numeric
     * precision at our maximum.
     *
     * @param metaData metadata for JDBC ResultSet
     * @param substitute if true, use substitutions; if false, throw exception
     * for unsupported types or type attributes
     * @param typeMapping types to substitute
     *
     * @return generated type
     */
    public RelDataType createResultSetType(
        ResultSetMetaData metaData,
        boolean substitute,
        Properties typeMapping);

    /**
     * Creates a type which represents column metadata returned by the {@link
     * DatabaseMetaData#getColumns} call. See {@link #createResultSetType} for
     * details on type substitutions.
     *
     * @param getColumnsResultSet {@link ResultSet}  positioned on a row
     * returned from the getColumns call; result set position is unchanged by
     * this method
     * @param substitute if true, use substitutions; if false, throw exception
     * for unsupported types or type attributes
     *
     * @return generated type
     */
    public RelDataType createJdbcColumnType(
        ResultSet getColumnsResultSet,
        boolean substitute);

    /**
     * Creates a type which represents column metadata returned by the {@link
     * DatabaseMetaData#getColumns} call. See {@link #createResultSetType} for
     * details on type substitutions.
     *
     * @param getColumnsResultSet {@link ResultSet}  positioned on a row
     * returned from the getColumns call; result set position is unchanged by
     * this method
     * @param substitute if true, use substitutions; if false, throw exception
     * for unsupported types or type attributes
     * @param typeMapping types to substitute
     *
     * @return generated type
     */
    public RelDataType createJdbcColumnType(
        ResultSet getColumnsResultSet,
        boolean substitute,
        Properties typeMapping);

    /**
     * Creates a type which represents a MOF feature.
     *
     * @param feature MOF feature
     *
     * @return generated type
     */
    public RelDataType createMofType(StructuralFeature feature);

    /**
     * Constructs an OpenJava expression to access a value of an atomic type.
     *
     * @param type atomic type
     * @param expr expression representing site to be accessed
     *
     * @return expression for accessing value
     */
    public Expression getValueAccessExpression(
        RelDataType type,
        Expression expr);

    /**
     * Looks up the {@link java.lang.Class} representing a primitive used to
     * hold a value of the given type.
     *
     * @param type value type
     *
     * @return primitive Class, or null if a non-primitive Object is used at
     * runtime to represent values of the given type
     */
    public Class getClassForPrimitive(
        RelDataType type);

    /**
     * Looks up the {@link java.lang.Class} specified by the JAVA parameter
     * style for user-defined routines.
     *
     * @param type SQL type
     *
     * @return corresponding Java class, or null if no mapping is defined
     */
    public Class getClassForJavaParamStyle(
        RelDataType type);
}

// End FarragoTypeFactory.java
