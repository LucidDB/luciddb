/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2007 The Eigenbase Project
// Copyright (C) 2007 SQLstream, Inc.
// Copyright (C) 2007 DynamoBI Corporation
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
package org.eigenbase.applib.util;

import java.io.*;

import java.net.*;

import java.sql.*;

import java.util.*;
import java.util.jar.*;

import net.sf.farrago.runtime.*;
import net.sf.farrago.util.*;

import org.eigenbase.applib.resource.*;
import org.eigenbase.util.*;


/**
 * Provides support for defining and enforcing constraints at the application
 * level
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class EnforceRowConstraintsUdx
{
    //~ Static fields/initializers ---------------------------------------------

    private static final String TAG_PREFIX = "EnforceRowConstraintsUDX_";
    private static final String RB_ATTRIBUTE_NAME = "ResourceBundleBaseName";
    private static final String DEFAULT_MESSAGE_CATALOG = "AppsMsg";
    private static final String EMPTY_STRING = "";

    private static final String [] SEV_STRING =
    { "WARNING", "REJECT", "FATAL" };

    private static final int WARNING = 0;
    private static final int REJECT = 1;
    private static final int FATAL = 2;

    //~ Methods ----------------------------------------------------------------

    public static void execute(
        ResultSet inputSet,
        List<String> constraintColumns,
        PreparedStatement resultInserter)
        throws ApplibException
    {
        execute(inputSet, constraintColumns, null, null, resultInserter);
    }

    public static void execute(
        ResultSet inputSet,
        List<String> constraintColumns,
        String resourceJarName,
        PreparedStatement resultInserter)
        throws ApplibException
    {
        execute(
            inputSet,
            constraintColumns,
            resourceJarName,
            null,
            resultInserter);
    }

    public static void execute(
        ResultSet inputSet,
        List<String> constraintColumns,
        String resourceJarName,
        String tag,
        PreparedStatement resultInserter)
        throws ApplibException
    {
        int nInput = 0;
        int nOutput = 0;

        try {
            nInput = inputSet.getMetaData().getColumnCount();
            nOutput = resultInserter.getParameterMetaData().getParameterCount();
        } catch (SQLException e) {
            throw ApplibResource.instance().InputOutputColumnError.ex(e);
        }
        int nConstraints = constraintColumns.size();

        // check number of input, output, and constraint columns
        assert ((nInput == nOutput) && (nInput > 0) && (nConstraints > 0));

        ResultSetMetaData rsmd;
        try {
            rsmd = inputSet.getMetaData();
        } catch (SQLException e) {
            throw ApplibResource.instance().CannotGetMetaData.ex(e);
        }
        verifyColumnReferenceList(constraintColumns, inputSet, rsmd);

        tag = (tag == null) ? generateTag() : tag;
        List<String> nonCheckColumns = null;
        ResourceBundle msgCatalog = null;
        resourceJarName =
            (resourceJarName == null) ? DEFAULT_MESSAGE_CATALOG
            : resourceJarName;
        try {
            // each row
            while (inputSet.next()) {
                RuntimeException deferedException = null;
                List<Object> nonCheckColumnValues = null;
                boolean keepRow = true;

                // loop through each constraint for a row
                for (int i = 0; i < nConstraints; i++) {
                    String errorCode =
                        inputSet.getString(constraintColumns.get(i));

                    // constraint needs to be checked
                    if (errorCode != null) {
                        msgCatalog =
                            (msgCatalog != null) ? msgCatalog
                            : getMessageCatalogResourceBundle(resourceJarName);
                        String errSev = null;
                        String errMsg = null;

                        // get non-constraint test column names & values
                        nonCheckColumns =
                            getNonCheckColumnNames(
                                nonCheckColumns,
                                constraintColumns,
                                rsmd,
                                nInput);
                        nonCheckColumnValues =
                            getNonCheckColumnValues(
                                nonCheckColumnValues,
                                nonCheckColumns,
                                inputSet);

                        // get error severity and message
                        try {
                            errSev =
                                msgCatalog.getString(errorCode + ".severity");
                        } catch (Exception e) {
                            // do nothing. checks are below
                        }
                        try {
                            errMsg =
                                expandColumns(
                                    msgCatalog.getString(errorCode + ".msg"),
                                    nonCheckColumns,
                                    nonCheckColumnValues);
                        } catch (Exception e) {
                            // do nothing. checks below
                        }
                        if (((errMsg == null) || (EMPTY_STRING.equals(errMsg)))
                            && ((errSev == null)
                                || (EMPTY_STRING.equals(errSev))))
                        {
                            errSev = SEV_STRING[WARNING];
                            errMsg =
                                "Error code '" + errorCode
                                + "' not found in message catalog '"
                                + resourceJarName + "'";
                        } else if (
                            ((errMsg != null)
                                && (!EMPTY_STRING.equals(errMsg)))
                            && ((errSev == null)
                                || (EMPTY_STRING.equals(errSev))))
                        {
                            errSev = SEV_STRING[WARNING];
                            errMsg =
                                "Severity for error code '" + errorCode
                                + "' not found in message catalog '"
                                + resourceJarName + "' Message: " + errMsg;
                        } else if (
                            ((errMsg == null)
                                || (EMPTY_STRING.equals(errMsg)))
                            && ((errSev != null)
                                && (!EMPTY_STRING.equals(errSev))))
                        {
                            errMsg =
                                "Message for error code '" + errorCode
                                + "' not found in message catalog '"
                                + resourceJarName + "'";
                        }

                        // if severity is FATAL log deferedException if one
                        // doesn't already exist
                        if ((errSev.equals(SEV_STRING[FATAL]))
                            && (deferedException == null))
                        {
                            deferedException =
                                (RuntimeException)
                                ApplibResource.instance().RowConstraintsFatal
                                .ex(
                                    errorCode,
                                    errMsg);
                        }

                        // filter rows which are reject and fatal
                        keepRow = (keepRow && isWarning(errSev));

                        Object ret =
                            FarragoUdrRuntime.handleRowError(
                                nonCheckColumns.toArray(
                                    new String[nonCheckColumns.size()]),
                                nonCheckColumnValues.toArray(),
                                new EigenbaseException(errMsg, null),
                                inputSet.findColumn(constraintColumns.get(i)),
                                tag,
                                isWarning(errSev),
                                errorCode,
                                constraintColumns.get(i));

                        if ((deferedException == null) && (ret != null)) {
                            if (ret instanceof RuntimeException) {
                                deferedException = (RuntimeException) ret;
                            }
                        }
                    } // end if error code exists
                } // end constraint loop

                FarragoUdrRuntime.handleRowErrorCompletion(
                    deferedException,
                    tag);
                insertRow(inputSet, resultInserter, nInput, keepRow);
            }

            inputSet.close();
        } catch (SQLException e) {
            throw ApplibResource.instance().DatabaseAccessError.ex(
                Util.getMessages(e),
                e);
        }
    }

    private static List<String> getNonCheckColumnNames(
        List<String> nonCheckColumns,
        List<String> constraintColumns,
        ResultSetMetaData rsmd,
        int numCols)
    {
        if (nonCheckColumns == null) {
            nonCheckColumns = new ArrayList();

            try {
                for (int i = 1; i <= numCols; i++) {
                    String colName = rsmd.getColumnName(i);
                    if (!constraintColumns.contains(colName)) {
                        nonCheckColumns.add(colName);
                    }
                }
            } catch (SQLException e) {
                throw ApplibResource.instance().DatabaseAccessError.ex(
                    Util.getMessages(e),
                    e);
            }
        }
        return nonCheckColumns;
    }

    private static void verifyColumnReferenceList(
        List<String> columnReferenceList,
        ResultSet inputSet,
        ResultSetMetaData rsmd)
        throws ApplibException
    {
        // check constraint columns exist in resultset and are of type VARCHAR
        Iterator<String> columnIter = columnReferenceList.iterator();
        String invalidColumns = null;
        SQLException unfoundColEx = null;
        while (columnIter.hasNext()) {
            String columnName = columnIter.next();
            try {
                int columnIndex = inputSet.findColumn(columnName);
                if (rsmd.getColumnType(columnIndex) != Types.VARCHAR) {
                    if (invalidColumns == null) {
                        invalidColumns = columnName;
                    } else {
                        invalidColumns = invalidColumns + ", " + columnName;
                    }
                }
            } catch (SQLException e) {
                if (invalidColumns == null) {
                    invalidColumns = columnName;
                    unfoundColEx = e;
                } else {
                    invalidColumns = invalidColumns + ", " + columnName;
                }
            }
        }
        if (invalidColumns != null) {
            throw ApplibResource.instance().InvalidCheckColumns.ex(
                invalidColumns,
                unfoundColEx);
        }
    }

    private static List<Object> getNonCheckColumnValues(
        List<Object> nonCheckColumnValues,
        List<String> nonCheckColumns,
        ResultSet rs)
    {
        if (nonCheckColumnValues == null) {
            nonCheckColumnValues = new ArrayList();
        }
        if (nonCheckColumnValues.isEmpty()) {
            ListIterator<String> nonCheckIter = nonCheckColumns.listIterator();
            while (nonCheckIter.hasNext()) {
                try {
                    nonCheckColumnValues.add(
                        rs.getObject(nonCheckIter.next()));
                } catch (SQLException e) {
                    throw ApplibResource.instance().DatabaseAccessError.ex(
                        Util.getMessages(e),
                        e);
                }
            }
        }
        return nonCheckColumnValues;
    }

    private static void insertRow(
        ResultSet inputSet,
        PreparedStatement resultInserter,
        int numCols,
        boolean doInsert)
    {
        if (doInsert) {
            try {
                for (int i = 1; i <= numCols; i++) {
                    Object obj = inputSet.getObject(i);
                    resultInserter.setObject(i, obj);
                }
                resultInserter.executeUpdate();
            } catch (SQLException ex) {
                throw ApplibResource.instance().DatabaseAccessError.ex(
                    Util.getMessages(ex),
                    ex);
            }
        }
    }

    private static String generateTag()
    {
        return TAG_PREFIX + UUID.randomUUID().toString() + "_"
            + Util.getFileTimestamp();
    }

    private static boolean isWarning(String severity)
    {
        if (severity.equals(SEV_STRING[WARNING])) {
            return true;
        } else {
            return false;
        }
    }

    private static String expandColumns(
        String msg,
        List<String> columnNames,
        List<Object> columnValues)
    {
        String expandedMsg = null;
        String [] tokens = msg.split("@");
        for (int i = 0; i < tokens.length; i++) {
            if (i == 0) {
                expandedMsg = tokens[i];
            } else if ((i % 2) == 0) {
                expandedMsg += tokens[i];
            } else {
                // get value for column
                int index = columnNames.indexOf(tokens[i]);
                if (index == -1) {
                    expandedMsg += tokens[i];
                } else {
                    if (columnValues.get(index) == null) {
                        expandedMsg += "null";
                    } else {
                        expandedMsg += columnValues.get(index).toString();
                    }
                }
            }
        }
        return expandedMsg;
    }

    private static ResourceBundle getMessageCatalogResourceBundle(
        String jarName)
        throws ApplibException
    {
        String homeDir = FarragoProperties.instance().homeDir.get(true);
        File pluginDir = new File(homeDir, "plugin");
        File messageJar = new File(pluginDir, jarName + ".jar");
        String pathToJar = null;
        try {
            pathToJar = messageJar.getCanonicalPath();
        } catch (java.io.IOException e) {
            throw ApplibResource.instance().MessageCatalogLoadFailed.ex(
                jarName,
                null,
                null,
                e.toString());
        }

        // check that file exists
        if (!messageJar.exists()) {
            throw ApplibResource.instance().NoSuchMessageCatalogJar.ex(
                jarName,
                pathToJar);
        }

        String rbBaseName = null;
        ResourceBundle rb = null;
        URL url = null;
        try {
            // get resource base name
            url = new URL("jar:file:" + pathToJar + "!/");
            JarURLConnection jarConnection =
                (JarURLConnection) url.openConnection();
            Manifest manifest = jarConnection.getManifest();
            rbBaseName =
                manifest.getMainAttributes().getValue(RB_ATTRIBUTE_NAME);

            // load resource from jar
            URL [] urls = new URL[1];
            urls[0] = url;
            URLClassLoader urlClassLoader = URLClassLoader.newInstance(urls);
            rb = ResourceBundle.getBundle(
                rbBaseName,
                Locale.getDefault(),
                urlClassLoader);
        } catch (Throwable ex) {
            throw ApplibResource.instance().MessageCatalogLoadFailed.ex(
                jarName,
                rbBaseName,
                pathToJar,
                ex.toString());
        }

        return rb;
    }
}

// End EnforceRowConstraintsUdx.java
