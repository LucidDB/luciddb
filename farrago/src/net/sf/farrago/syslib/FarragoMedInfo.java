package net.sf.farrago.syslib;

import java.io.*;
import java.sql.*;
import java.util.*;
import net.sf.farrago.db.*;
import net.sf.farrago.namespace.FarragoMedDataWrapper;
import net.sf.farrago.namespace.util.FarragoDataWrapperCache;
import net.sf.farrago.runtime.FarragoUdrRuntime;
import net.sf.farrago.util.FarragoObjectCache;


public class FarragoMedInfo {
    public static final int  NAME = 1;
    public static final int  AVALUE = 2;
    public static final int  DESCRIPTION = 3;
    public static final int  CHOICES = 4;
    public static final int  REQUIRED = 5;
    public static final int  ISFOREIGN = 1;
    protected static FarragoDataWrapperCache  dataWrapperCache = null;


public static void getPluginPropertyInfo(
        String mofIdArg,
        String libraryNameArg,
        String optionsArg,
        String wrapperPropertiesArg,
        String localeArg,
        PreparedStatement resultInserter)
        throws SQLException
    {
        Properties  options = new Properties(
            getPropertiesFromString(optionsArg));
        Locale  locale = toLocale(localeArg);
        Properties  wrapperProperties = getPropertiesFromString(
            wrapperPropertiesArg);
        FarragoMedDataWrapper  dataWrapper = getWrapper(
            mofIdArg,
            libraryNameArg,
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


    public static void getServerPropertyInfo(
        String mofIdArg,
        String libraryNameArg,
        String optionsArg,
        String wrapperPropertiesArg,
        String serverPropertiesArg,
        String localeArg,
        PreparedStatement resultInserter)
        throws SQLException
    {
        Properties  options = new Properties(
            getPropertiesFromString(optionsArg));
        Locale  locale = toLocale(localeArg);
        Properties  wrapperProperties = getPropertiesFromString(
            wrapperPropertiesArg);
        FarragoMedDataWrapper  dataWrapper = getWrapper(
            mofIdArg,
            libraryNameArg,
            options);
        Properties  serverProperties = getPropertiesFromString(
            serverPropertiesArg);
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


    public static void getColumnSetPropertyInfo(
        String mofIdArg,
        String libraryNameArg,
        String optionsArg,
        String wrapperPropertiesArg,
        String serverPropertiesArg,
        String tablePropertiesArg,
        String localeArg,
        PreparedStatement resultInserter)
        throws SQLException
    {
        Properties  options = new Properties(
                getPropertiesFromString(optionsArg));
        Locale  locale = toLocale(localeArg);
        Properties  wrapperProperties = getPropertiesFromString(
            wrapperPropertiesArg);
        FarragoMedDataWrapper  dataWrapper = getWrapper(
            mofIdArg,
            libraryNameArg,
            options);
        Properties  serverProperties = getPropertiesFromString(
            serverPropertiesArg);
        Properties  tableProperties = getPropertiesFromString(
            tablePropertiesArg);
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


    public static void getColumnPropertyInfo(
        String mofIdArg,
        String libraryNameArg,
        String optionsArg,
        String wrapperPropertiesArg,
        String serverPropertiesArg,
        String tablePropertiesArg,
        String columnPropertiesArg,
        String localeArg,
        PreparedStatement resultInserter)
        throws SQLException
    {
        Properties  options = new Properties(
            getPropertiesFromString(optionsArg));
        Locale  locale = toLocale(localeArg);
        Properties  wrapperProperties = getPropertiesFromString(
            wrapperPropertiesArg);
        FarragoMedDataWrapper  dataWrapper = getWrapper(
            mofIdArg,
            libraryNameArg,
            options);
        Properties  serverProperties = getPropertiesFromString(
            serverPropertiesArg);
        Properties  tableProperties = getPropertiesFromString(
            tablePropertiesArg);
        Properties  columnProperties = getPropertiesFromString(
            columnPropertiesArg);
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


    public static void isForeign(
        String mofIdArg,
        String libraryNameArg,
        String optionsArg,
        String localeArg, // TODO Should this be removed?
        PreparedStatement resultInserter)
        throws SQLException
    {
        Properties  options = new Properties(
            getPropertiesFromString(optionsArg));
        FarragoMedDataWrapper  dataWrapper = getWrapper(
            mofIdArg,
            libraryNameArg,
            options);
        try {
            boolean  isForeign = dataWrapper.isForeign ();
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
     static public String getExceptionStackTraceAsString(Throwable exception)
     {
         StringWriter stringWriter = new StringWriter();
         PrintWriter printWriter = new PrintWriter(stringWriter);
         exception.printStackTrace(printWriter);
         return stringWriter.toString();
     }


    /** Get a FarragoMedDataWrapper based on the data passed in.
     * @param mofIdArg The Meta object ID of the wrapper to get.
     * @param libraryNameArg The library name of the wrapper to get.
     * @param optionsArg The options to pass to the wrapper.
     * @return The FarragoMedDataWrapper that matches the args passed in. */
    public static FarragoMedDataWrapper getWrapper(
        String mofIdArg,
        String libraryNameArg,
        Properties optionsArg)
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
            mofIdArg,
            libraryNameArg,
            optionsArg);
    }


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
    public static String putPropertiesToString(Properties properties) {
        String  result = new String();
        for (Object key : Collections.list(properties.keys())) {
            result += key + "\t" + properties.get(key) + "\n";
        }
        return result;
    }


    /** Convert a String version of a Locale to an SQL Locale.
     * @param localeString The locale String to convert.
     * @return The Locale based on the String passed in. */
    public static Locale toLocale(String localeString) {
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
                    choices.put (oneChoice, oneChoice);
                }
            }
            resultInserter.setString(CHOICES, putPropertiesToString(choices));
            resultInserter.setBoolean(REQUIRED, oneProperty.required);
            resultInserter.executeUpdate();
        }
    }
}

