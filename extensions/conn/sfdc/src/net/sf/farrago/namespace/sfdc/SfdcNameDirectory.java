/*
// $Id$
// SFDC Connector is an Eigenbase SQL/MED connector for Salesforce.com
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */
package net.sf.farrago.namespace.sfdc;

import com.sforce.soap.partner.*;

import java.io.StringWriter;

import java.rmi.RemoteException;

import java.sql.*;

import java.util.*;
import java.util.regex.Pattern;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.namespace.sfdc.resource.*;
import net.sf.farrago.type.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.type.SqlTypeName;


/**
 * SfdcNameDirectory provides an implementation of the {@link
 * FarragoMedNameDirectory} interface.
 *
 * @author Sunny Choi
 * @version $Id$
 */
class SfdcNameDirectory
    extends MedAbstractNameDirectory
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Properties EMPTY_PROPERTIES = new Properties();
    protected static final int MAX_PRECISION = 256;

    protected static final Log log = LogFactory.getLog(SfdcNameDirectory.class);

    //~ Instance fields --------------------------------------------------------

    // ~ Instance fields -------------------------------------------------------

    final SfdcDataServer server;
    String scope;
    int varcharPrecision;
    private final Map<String, Pattern> patternMap;

    //~ Constructors -----------------------------------------------------------

    // ~ Constructors ----------------------------------------------------------

    SfdcNameDirectory(SfdcDataServer server, String scope)
    {
        this.server = server;
        this.scope = scope;
        this.varcharPrecision = this.server.getVarcharPrecision();
        this.patternMap = new HashMap<String, Pattern>();
    }

    //~ Methods ----------------------------------------------------------------

    // ~ Methods ---------------------------------------------------------------

    // implement FarragoMedNameDirectory
    public FarragoMedColumnSet lookupColumnSet(
        FarragoTypeFactory typeFactory,
        String foreignName,
        String [] localName)
        throws SQLException
    {
        if (!scope.equals(FarragoMedMetadataQuery.OTN_TABLE)) {
            return null;
        }

        return server.newColumnSet(
            localName,
            server.getProperties(),
            typeFactory,
            null,
            Collections.EMPTY_MAP);
    }

    // implement FarragoMedNameDirectory
    public FarragoMedNameDirectory lookupSubdirectory(String foreignName)
        throws SQLException
    {
        if (scope.equals(FarragoMedMetadataQuery.OTN_SCHEMA)) {
            if (!foreignName.equals("SFDC") && !foreignName.equals("DEFAULT")) {
                return null;
            }
            return new SfdcNameDirectory(
                server,
                FarragoMedMetadataQuery.OTN_TABLE);
        }
        return null;
    }

    // implement FarragoMedNameDirectory
    public boolean queryMetadata(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink)
        throws SQLException
    {
        if (scope.equals(FarragoMedMetadataQuery.OTN_SCHEMA)) {
            boolean wantSchemas =
                query.getResultObjectTypes().contains(
                    FarragoMedMetadataQuery.OTN_SCHEMA);
            if (wantSchemas) {
                sink.writeObjectDescriptor(
                    "SFDC",
                    FarragoMedMetadataQuery.OTN_SCHEMA,
                    null,
                    EMPTY_PROPERTIES);
            }
        } else {
            boolean wantTables =
                query.getResultObjectTypes().contains(
                    FarragoMedMetadataQuery.OTN_TABLE);
            if (wantTables) {
                if (!queryTables(query, sink)) {
                    return false;
                }
            }
            boolean wantColumns =
                query.getResultObjectTypes().contains(
                    FarragoMedMetadataQuery.OTN_COLUMN);
            if (wantColumns) {
                if (!queryColumns(query, sink)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean queryTables(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink)
        throws SQLException
    {
        try {
            DescribeGlobalResult describeGlobalResult = server.getEntityTypes();
            if (!(describeGlobalResult == null)) {
                DescribeGlobalSObjectResult [] types =
                    describeGlobalResult.getSobjects();
                if (!(types == null)) {
                    for (int i = 0; i < types.length; i++) {
                        String tableName = types[i].getName();
                        if (isIncluded(tableName, query)) {
                            sink.writeObjectDescriptor(
                                tableName,
                                FarragoMedMetadataQuery.OTN_TABLE,
                                null,
                                EMPTY_PROPERTIES);
                        }
                        if (isLovIncluded(tableName, query)) {
                            sink.writeObjectDescriptor(
                                tableName + "_LOV",
                                FarragoMedMetadataQuery.OTN_TABLE,
                                null,
                                EMPTY_PROPERTIES);
                        }
                    }
                }
            }
        } catch (RemoteException re) {
            throw SfdcResourceObject.get().BindingCallException.ex(
                re.getMessage());
        }
        return true;
    }

    private boolean queryColumns(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink)
        throws SQLException
    {
        try {
            DescribeGlobalResult describeGlobalResult = server.getEntityTypes();
            if (!(describeGlobalResult == null)) {
                // Get the array of object names
                DescribeGlobalSObjectResult [] types =
                    describeGlobalResult.getSobjects();
                for (int i = 0; i < types.length; i++) {
                    if (!isIncluded(types[i].getName(), query)
                        && !isLovIncluded(types[i].getName(), query))
                    {
                        continue;
                    }

                    // verify can access objects
                    DescribeSObjectResult describeSObjectResult =
                        (DescribeSObjectResult) server.getEntityDescribe(
                            types[i].getName());

                    // check the name
                    if ((describeSObjectResult != null)
                        && describeSObjectResult.getName().equals(
                            types[i].getName()))
                    {
                        if (isIncluded(types[i].getName(), query)) {
                            com.sforce.soap.partner.Field [] fields =
                                describeSObjectResult.getFields();
                            int ordinal = 0;
                            for (int j = 0; j < fields.length; j++) {
                                RelDataType reltype =
                                    toRelType(
                                        fields[j],
                                        sink.getTypeFactory());
                                sink.writeColumnDescriptor(
                                    types[i].getName(),
                                    fields[j].getName(),
                                    ordinal,
                                    reltype,
                                    null,
                                    null,
                                    EMPTY_PROPERTIES);
                                ordinal++;
                            }
                        }
                        if (isLovIncluded(types[i].getName(), query)) {
                            // import picklist LOV as well
                            RelDataType reltype =
                                sink.getTypeFactory().createTypeWithNullability(
                                    sink.getTypeFactory().createSqlType(
                                        SqlTypeName.VARCHAR,
                                        25 + this.varcharPrecision),
                                    true);
                            sink.writeColumnDescriptor(
                                types[i].getName() + "_LOV",
                                "Field",
                                0,
                                reltype,
                                null,
                                null,
                                EMPTY_PROPERTIES);
                            reltype =
                                sink.getTypeFactory().createTypeWithNullability(
                                    sink.getTypeFactory().createSqlType(
                                        SqlTypeName.VARCHAR,
                                        MAX_PRECISION + this.varcharPrecision),
                                    true);
                            sink.writeColumnDescriptor(
                                types[i].getName() + "_LOV",
                                "Value",
                                1,
                                reltype,
                                null,
                                null,
                                EMPTY_PROPERTIES);
                        }
                    } else {
                        if (types[i].getName().endsWith("__c")) {
                            log.info(
                                SfdcResourceObject.get().ObjectExtractErrorMsg
                                .str(types[i].getName()));
                        } else {
                            throw SfdcResourceObject.get()
                            .InvalidObjectException.ex(types[i].getName());
                        }
                    }
                }
            }
        } catch (RemoteException re) {
            throw SfdcResourceObject.get().BindingCallException.ex(
                re.getMessage());
        }
        return true;
    }

    private boolean isIncluded(String tableName, FarragoMedMetadataQuery query)
    {
        FarragoMedMetadataFilter filter =
            (FarragoMedMetadataFilter) query.getFilterMap().get(
                FarragoMedMetadataQuery.OTN_TABLE);
        if (filter == null) {
            return true;
        }
        boolean included = false;
        if (filter.getRoster() != null) {
            if (filter.getRoster().contains(tableName)) {
                included = true;
            }
        } else {
            Pattern pattern = getPattern(filter.getPattern());
            included = pattern.matcher(tableName).matches();
        }
        if (filter.isExclusion()) {
            included = !included;
        }
        return included;
    }

    private boolean isLovIncluded(
        String tableName,
        FarragoMedMetadataQuery query)
    {
        return isIncluded(tableName + "_LOV", query);
    }

    // See: MedAbstractMetadataSink.getPattern()
    Pattern getPattern(String likePattern)
    {
        Pattern pattern = patternMap.get(likePattern);
        if (pattern == null) {
            StringWriter regex = new StringWriter();
            int n = likePattern.length();
            for (int i = 0; i < n; ++i) {
                char c = likePattern.charAt(i);
                switch (c) {
                case '%':
                    regex.write(".*");
                    break;
                case '_':
                    regex.write(".");
                    break;
                default:
                    regex.write((int) c);
                    break;
                }
            }
            pattern = Pattern.compile(regex.toString());
            patternMap.put(likePattern, pattern);
        }
        return pattern;
    }

    protected RelDataType toRelType(Field field, FarragoTypeFactory typeFactory)
    {
        SqlTypeName typeName = convertSfdcSqlToSqlType(field.getType());
        if (typeName.allowsPrec()) {
            if (typeName.allowsScale()) { // only decimal type
                int maxPrecision = SqlTypeName.DECIMAL.MAX_NUMERIC_PRECISION;
                int precision = field.getPrecision();
                int scale = field.getScale();
                if ((precision > maxPrecision) || (scale > precision)) {
                    precision = maxPrecision;
                    int cappedScale = 6;
                    if (scale > cappedScale) {
                        scale = cappedScale;
                    }
                }
                if (scale < 0) {
                    scale = 0;
                }
                return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(typeName, precision, scale),
                    true);
            } else {
                int precision = field.getPrecision();
                String fieldType = field.getType().getValue().toLowerCase();
                if (typeName.equals(SqlTypeName.TIMESTAMP)
                    || typeName.equals(SqlTypeName.TIME))
                {
                    precision = typeName.getDefaultPrecision();
                } else if (fieldType.equals("boolean")) {
                    precision = 5;
                } else {
                    precision = field.getLength() + varcharPrecision;
                    if (precision == 0) {
                        precision = 1024;
                    }
                }

                if (precision > (MAX_PRECISION + varcharPrecision)) {
                    precision = MAX_PRECISION + varcharPrecision;
                }

                return typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(typeName, precision),
                    true);
            }
        } else {
            return typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(typeName),
                true);
        }
    }

    private static SqlTypeName convertSfdcSqlToSqlType(FieldType field)
    {
        if (field.getValue().equalsIgnoreCase("string")
            || field.getValue().equalsIgnoreCase("boolean")
            || field.getValue().equalsIgnoreCase("base64")
            || field.getValue().equalsIgnoreCase("id")
            || field.getValue().equalsIgnoreCase("reference")
            || field.getValue().equalsIgnoreCase("textarea")
            || field.getValue().equalsIgnoreCase("phone")
            || field.getValue().equalsIgnoreCase("url")
            || field.getValue().equalsIgnoreCase("email")
            || field.getValue().equalsIgnoreCase("picklist")
            || field.getValue().equalsIgnoreCase("multipicklist")
            || field.getValue().equalsIgnoreCase("combobox")
            || field.getValue().equalsIgnoreCase("calculated")
            || field.getValue().equalsIgnoreCase("anytype"))
        {
            return SqlTypeName.VARCHAR;
        } else if (field.getValue().equalsIgnoreCase("int")) {
            return SqlTypeName.INTEGER;
        } else if (
            field.getValue().equalsIgnoreCase("double")
            || field.getValue().equalsIgnoreCase("currency")
            || field.getValue().equalsIgnoreCase("percent"))
        {
            return SqlTypeName.DOUBLE;
        } else if (field.getValue().equalsIgnoreCase("date")) {
            return SqlTypeName.DATE;
        } else if (field.getValue().equalsIgnoreCase("datetime")) {
            return SqlTypeName.TIMESTAMP;
        } else if (field.getValue().equalsIgnoreCase("time")) {
            return SqlTypeName.TIME;
        } else { // unknown
            return SqlTypeName.VARCHAR;
        }
    }
}

// End SfdcNameDirectory.java
