/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 Dynamo BI Corporation
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
package net.sf.farrago.syslib;

import java.io.*;
import java.sql.*;
import java.util.*;
import net.sf.farrago.db.*;
import net.sf.farrago.namespace.FarragoMedDataWrapper;
import net.sf.farrago.namespace.util.FarragoDataWrapperCache;
import net.sf.farrago.runtime.FarragoUdrRuntime;
import net.sf.farrago.util.FarragoObjectCache;


/** A class to get certain Farrago Med property values.  The UDX functions
 * in initsql cause these functions to be invoked. */
public class FarragoMedInfo {
    public static final int  NAME = 1;
    public static final int  AVALUE = 2;
    public static final int  DESCRIPTION = 3;
    public static final int  CHOICES = 4;
    public static final int  REQUIRED = 5;
    public static final int  ISFOREIGN = 1;
    protected static FarragoDataWrapperCache  dataWrapperCache = null;


    /**
     * Get the plugin property info for the mofId, library combo passed in.
     * @param mofId The mofId to get property info for.
     * @param libraryName The library to get property info for.
     * @param optionsArg The options to pass to the data wrapper.
     * @param wrapperPropertiesString The wrapper props to request.
     * @param localeArg What locale to get properties for.
     * @param resultInserter Where the result rows are placed.
     * @throws SQLException Thrown on repo access failure.
     */
public static void getPluginPropertyInfo(
        String mofId,
        String libraryName,
        String optionsArg,
        String wrapperPropertiesString,
        String localeArg,
        PreparedStatement resultInserter)
        throws SQLException
    {
        Properties  options = new Properties(
            getPropertiesFromString(optionsArg));
        Locale  locale = toLocale(localeArg);
        Properties  wrapperProperties = getPropertiesFromString(
            wrapperPropertiesString);
        FarragoMedDataWrapper  dataWrapper = getWrapper(
            mofId,
            libraryName,
            options);
        DriverPropertyInfo[]  driverPropertyInfo;
        try {
            driverPropertyInfo = dataWrapper.getPluginPropertyInfo(
                locale,
                wrapperProperties);
        } finally {
            closeWrapperCache();
        }
        moveDriverPropertyInfoToResult(driverPropertyInfo, resultInserter);
    }


    /**
     * Get the server property info for the mofId, library combo passed in.
     * @param mofId The mofId to get property info for.
     * @param libraryName The library to get property info for.
     * @param optionsArg The options to pass to the data wrapper.
     * @param wrapperPropertiesString The wrapper props to request.
     * @param serverPropertiesString The server props to request.
     * @param localeArg What locale to get properties for.
     * @param resultInserter Where the result rows are placed.
     * @throws SQLException Thrown on repo access failure.
     */
    public static void getServerPropertyInfo(
        String mofId,
        String libraryName,
        String optionsArg,
        String wrapperPropertiesString,
        String serverPropertiesString,
        String localeArg,
        PreparedStatement resultInserter)
        throws SQLException
    {
        Properties  options = new Properties(
            getPropertiesFromString(optionsArg));
        Locale  locale = toLocale(localeArg);
        Properties  wrapperProperties = getPropertiesFromString(
            wrapperPropertiesString);
        FarragoMedDataWrapper  dataWrapper = getWrapper(
            mofId,
            libraryName,
            options);
        Properties  serverProperties = getPropertiesFromString(
            serverPropertiesString);
        DriverPropertyInfo[] driverPropertyInfo;
        try {
            driverPropertyInfo = dataWrapper.getServerPropertyInfo(
                locale,
                wrapperProperties,
                serverProperties);
        } finally {
            closeWrapperCache();
        }
        moveDriverPropertyInfoToResult(driverPropertyInfo, resultInserter);
    }


    /**
     * Get the column set property info for the mofId, library combo passed in.
     * @param mofId The mofId to get property info for.
     * @param libraryName The library to get property info for.
     * @param optionsString The options to pass to the data wrapper.
     * @param wrapperPropertiesString The wrapper props to request.
     * @param serverPropertiesString The table props to request.
     * @param tablePropertiesString The table props to request.
     * @param localeArg What locale to get properties for.
     * @param resultInserter Where the result rows are placed.
     * @throws SQLException Thrown on repo access failure.
     */
    public static void getColumnSetPropertyInfo(
        String mofId,
        String libraryName,
        String optionsString,
        String wrapperPropertiesString,
        String serverPropertiesString,
        String tablePropertiesString,
        String localeArg,
        PreparedStatement resultInserter)
        throws SQLException
    {
        Properties  options = new Properties(
                getPropertiesFromString(optionsString));
        Locale  locale = toLocale(localeArg);
        Properties  wrapperProperties = getPropertiesFromString(
            wrapperPropertiesString);
        FarragoMedDataWrapper  dataWrapper = getWrapper(
            mofId,
            libraryName,
            options);
        Properties  serverProperties = getPropertiesFromString(
            serverPropertiesString);
        Properties  tableProperties = getPropertiesFromString(
            tablePropertiesString);
        DriverPropertyInfo[] driverPropertyInfo;
        try {
            driverPropertyInfo = dataWrapper.getColumnSetPropertyInfo(
                locale,
                wrapperProperties,
                serverProperties,
                tableProperties);
        } finally {
            closeWrapperCache();
        }
        moveDriverPropertyInfoToResult(driverPropertyInfo, resultInserter);
    }


    /**
     * Get the column property info for the mofId, library combo passed in.
     * @param mofId The mofId to get property info for.
     * @param libraryName The library to get property info for.
     * @param optionsString The options to pass to the data wrapper.
     * @param wrapperPropertiesString The wrapper props to request.
     * @param serverPropertiesString The table props to request.
     * @param tablePropertiesString The table props to request.
     * @param columnPropertiesString The column props to request.
     * @param localeArg What locale to get properties for.
     * @param resultInserter Where the result rows are placed.
     * @throws SQLException Thrown on repo access failure.
     */
    public static void getColumnPropertyInfo(
        String mofId,
        String libraryName,
        String optionsString,
        String wrapperPropertiesString,
        String serverPropertiesString,
        String tablePropertiesString,
        String columnPropertiesString,
        String localeArg,
        PreparedStatement resultInserter)
        throws SQLException
    {
        Properties  options = new Properties(
            getPropertiesFromString(optionsString));
        Locale  locale = toLocale(localeArg);
        Properties  wrapperProperties = getPropertiesFromString(
            wrapperPropertiesString);
        FarragoMedDataWrapper  dataWrapper = getWrapper(
            mofId,
            libraryName,
            options);
        Properties  serverProperties = getPropertiesFromString(
            serverPropertiesString);
        Properties  tableProperties = getPropertiesFromString(
            tablePropertiesString);
        Properties  columnProperties = getPropertiesFromString(
            columnPropertiesString);
        DriverPropertyInfo[] driverPropertyInfo;
        try {
            driverPropertyInfo = dataWrapper.getColumnPropertyInfo(
                locale,
                wrapperProperties,
                serverProperties,
                tableProperties,
                columnProperties);
        } finally {
            closeWrapperCache();
        }
        moveDriverPropertyInfoToResult(driverPropertyInfo, resultInserter);
    }


    /**
     * Is the library object foreign?
     * @param mofId The id to check.
     * @param libraryName The library to check.
     * @param optionsString The options to pass to the data wrapper.
     * @param localeString What locale to check for.
     * @param resultInserter Where the result rows are placed.
     * @throws SQLException Thrown on repo access failure.
     */
    public static void isForeign(
        String mofId,
        String libraryName,
        String optionsString,
        String localeString, // TODO Should this be removed?
        PreparedStatement resultInserter)
        throws SQLException
    {
        Properties  options = new Properties(
            getPropertiesFromString(optionsString));
        FarragoMedDataWrapper  dataWrapper = getWrapper(
            mofId,
            libraryName,
            options);
        try {
            boolean  isForeign = dataWrapper.isForeign();
            resultInserter.setBoolean(ISFOREIGN, isForeign);
            resultInserter.executeUpdate();
            // TODO Can we skip the result update when there is an exception?
        } finally {
            closeWrapperCache();
        }
    }


    /** Get an exception's stack trace as a String.
     * @param exception The exception to get the stack trace for.
     * @return A String version of the stack trace of the exception
     * parameter. */
    public static String getExceptionStackTraceAsString(Throwable exception)
    {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        exception.printStackTrace(printWriter);
        return stringWriter.toString();
    }


    /** Get a FarragoMedDataWrapper based on the data passed in.
     * @param mofId The Meta object ID of the wrapper to get.
     * @param libraryName The library name of the wrapper to get.
     * @param options The options to pass to the wrapper.
     * @return The FarragoMedDataWrapper that matches the args passed in. */
    public static FarragoMedDataWrapper getWrapper(
        String mofId,
        String libraryName,
        Properties options)
    {
        closeWrapperCache();
        FarragoDbSession  session =
            (FarragoDbSession) FarragoUdrRuntime.getSession();
        FarragoDatabase  db = session.getDatabase();
        FarragoObjectCache  sharedCache = db.getDataWrapperCache();
        dataWrapperCache = session.newFarragoDataWrapperCache(
            session,
            sharedCache,
            session.getRepos(),
            db.getFennelDbHandle(),
            null);
        return dataWrapperCache.loadWrapper(
            mofId,
            libraryName,
            options);
    }


    /** Close the wrapper cache. */
    public static void closeWrapperCache()
    {
        if (dataWrapperCache != null) {
            dataWrapperCache.closeAllocation();
            dataWrapperCache = null;
        }
    }


    /** Parse a String formatted as putPropertiesToString generates to create
        a Properties table.  It is assumed that the input String is
        well-formed.
     @param propertiesString Properties in a tab and CR delimited String.
     @return Properties as an object. */
    public static Properties getPropertiesFromString(
        String propertiesString)
    {
        Properties  result = new Properties();
        String[] propertyList = propertiesString.split("\n");
        for (String  property : propertyList) {
            String  keyAndValue[] = property.split("\t");
            // An empty String can get into loop and must be skipped
            if (keyAndValue.length > 1) {
                result.put(keyAndValue[0], keyAndValue[1]);
            }
        }
        return result;
    }


    /** Produces output that looks like this (where a single space is a tab):
      "key1 value1
       key2 value2
      "
      @param properties A standard Java Property set that will be converted to
      a String (formatted as described above).
      @return A String containing the contents of the passed Property set. */
    public static String putPropertiesToString(
        Properties properties)
    {
        String  result = new String();
        for (Object key : Collections.list(properties.keys())) {
            result += key + "\t" + properties.get(key) + "\n";
        }
        return result;
    }


    /** Convert a String version of a Locale to an SQL Locale.
     * @param localeString The locale String to convert.
     * @return The Locale based on the String passed in. */
    public static Locale toLocale(String localeString)
    {
        return new Locale(localeString);
    }


    /** Move the driver property info array to an SQL result.
     * @param driverPropertyInfo The array to convert.
     * @param resultInserter Where to put the result.
     * @throws SQLException If there's a problem writing the result. */
    public static void moveDriverPropertyInfoToResult(
            DriverPropertyInfo[] driverPropertyInfo,
            PreparedStatement resultInserter) throws SQLException
    {
        for (DriverPropertyInfo oneProperty : driverPropertyInfo) {
            resultInserter.setString(NAME, oneProperty.name);
            resultInserter.setString(AVALUE, oneProperty.value);
            resultInserter.setString(DESCRIPTION, oneProperty.description);
            // Just treat choices like properties for simplicity
            Properties  choices = new Properties();
            if (oneProperty.choices != null) {
                for (String oneChoice : oneProperty.choices) {
                    choices.put(oneChoice, oneChoice);
                }
            }
            resultInserter.setString(CHOICES, putPropertiesToString(choices));
            resultInserter.setBoolean(REQUIRED, oneProperty.required);
            resultInserter.executeUpdate();
        }
    }
}

// End FarragoMedInfo.java
