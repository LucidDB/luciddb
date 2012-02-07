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
package net.sf.farrago.namespace.flatfile;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;


/**
 * FlatFileDataWrapper provides an implementation of the {@link
 * FarragoMedDataWrapper} interface for reading from flat files.
 *
 * @author John V. Pham
 * @version $Id$
 */
public class FlatFileDataWrapper
    extends MedAbstractDataWrapper
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new data wrapper instance.
     */
    public FlatFileDataWrapper()
    {
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoMedDataWrapper
    public String getSuggestedName()
    {
        return "FLATFILE_DATA_WRAPPER";
    }

    // implement FarragoMedDataWrapper
    public String getDescription(Locale locale)
    {
        // TODO: localize
        return "Foreign data wrapper for flatfile tables";
    }

    // implement FarragoMedDataWrapper
    public DriverPropertyInfo [] getServerPropertyInfo(
        Locale locale,
        Properties wrapperProps,
        Properties serverProps)
    {
        // TODO:  use locale

        MedPropertyInfoMap infoMap =
            new MedPropertyInfoMap(
                FarragoResource.instance(),
                "MedFlatFile",
                serverProps);
        infoMap.addPropInfo(
            FlatFileParams.PROP_DIRECTORY,
            true);
        infoMap.addPropInfo(
            FlatFileParams.PROP_FILE_EXTENSION,
            true,
            new String[] { FlatFileParams.DEFAULT_FILE_EXTENSION });
        infoMap.addPropInfo(
            FlatFileParams.PROP_CONTROL_FILE_EXTENSION,
            true,
            new String[] { FlatFileParams.DEFAULT_CONTROL_FILE_EXTENSION });
        infoMap.addPropInfo(
            FlatFileParams.PROP_FIELD_DELIMITER,
            true,
            new String[] { FlatFileParams.DEFAULT_FIELD_DELIMITER });
        infoMap.addPropInfo(
            FlatFileParams.PROP_LINE_DELIMITER,
            true,
            new String[] { FlatFileParams.DEFAULT_LINE_DELIMITER });
        infoMap.addPropInfo(
            FlatFileParams.PROP_QUOTE_CHAR,
            true,
            new String[] { FlatFileParams.DEFAULT_QUOTE_CHAR });
        infoMap.addPropInfo(
            FlatFileParams.PROP_ESCAPE_CHAR,
            true,
            new String[] { FlatFileParams.DEFAULT_ESCAPE_CHAR });
        if (FlatFileParams.DEFAULT_WITH_HEADER) {
            infoMap.addPropInfo(
                FlatFileParams.PROP_WITH_HEADER,
                true,
                BOOLEAN_CHOICES_DEFAULT_TRUE);
        } else {
            infoMap.addPropInfo(
                FlatFileParams.PROP_WITH_HEADER,
                true,
                BOOLEAN_CHOICES_DEFAULT_FALSE);
        }
        infoMap.addPropInfo(
            FlatFileParams.PROP_NUM_ROWS_SCAN,
            true,
            new String[] {
                Integer.toString(FlatFileParams.DEFAULT_NUM_ROWS_SCAN)
            });
        if (FlatFileParams.DEFAULT_TRIM) {
            infoMap.addPropInfo(
                FlatFileParams.PROP_TRIM,
                true,
                BOOLEAN_CHOICES_DEFAULT_TRUE);
        } else {
            infoMap.addPropInfo(
                FlatFileParams.PROP_TRIM,
                true,
                BOOLEAN_CHOICES_DEFAULT_FALSE);
        }
        if (FlatFileParams.DEFAULT_LENIENT) {
            infoMap.addPropInfo(
                FlatFileParams.PROP_LENIENT,
                true,
                BOOLEAN_CHOICES_DEFAULT_TRUE);
        } else {
            infoMap.addPropInfo(
                FlatFileParams.PROP_LENIENT,
                true,
                BOOLEAN_CHOICES_DEFAULT_FALSE);
        }
        if (FlatFileParams.DEFAULT_MAPPED) {
            infoMap.addPropInfo(
                FlatFileParams.PROP_MAPPED,
                true,
                BOOLEAN_CHOICES_DEFAULT_TRUE);
        } else {
            infoMap.addPropInfo(
                FlatFileParams.PROP_MAPPED,
                true,
                BOOLEAN_CHOICES_DEFAULT_FALSE);
        }
        infoMap.addPropInfo(
            FlatFileParams.PROP_DATE_FORMAT,
            false);
        infoMap.addPropInfo(
            FlatFileParams.PROP_TIME_FORMAT,
            false);
        infoMap.addPropInfo(
            FlatFileParams.PROP_TIMESTAMP_FORMAT,
            false);
        return infoMap.toArray();
    }

    // TODO:  DriverPropertyInfo calls
    // implement FarragoMedDataWrapper
    public void initialize(
        FarragoRepos repos,
        Properties props)
        throws SQLException
    {
        super.initialize(repos, props);
    }

    // implement FarragoMedDataWrapper
    public FarragoMedDataServer newServer(
        String serverMofId,
        Properties props)
        throws SQLException
    {
        FlatFileDataServer server =
            new FlatFileDataServer(
                this,
                serverMofId,
                props);
        boolean success = false;
        try {
            server.initialize();
            success = true;
            return server;
        } finally {
            if (!success) {
                server.closeAllocation();
            }
        }
    }
}

// End FlatFileDataWrapper.java
