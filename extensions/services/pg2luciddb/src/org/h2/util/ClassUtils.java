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
package org.h2.util;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;

import org.h2.constant.ErrorCode;
import org.h2.message.Message;

/**
 * This utility class contains functions related to class loading.
 * There is a mechanism to restrict class loading.
 */
public class ClassUtils 
{

    private ClassUtils() 
    {
        // utility class
    }

    /**
     * Load a class without performing access rights checking.
     *
     * @param className the name of the class
     * @return the class object
     */
    public static Class< ? > loadSystemClass(String className) throws ClassNotFoundException {
        return Class.forName(className);
    }

    /**
     * Load a class, but check if it is allowed to load this class first. To
     * perform access rights checking, the system property h2.allowedClasses
     * needs to be set to a list of class file name prefixes.
     *
     * @param className the name of the class
     * @return the class object
     */
    public static Class< ? > loadUserClass(String className) throws SQLException {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw Message.getSQLException(344, new String[] { className }, e);
        } catch (NoClassDefFoundError e) {
            throw Message.getSQLException(344, new String[] { className }, e);
        }
    }

    /**
     * Checks if the given method takes a variable number of arguments. For Java
     * 1.4 and older, false is returned. Example:
     * <pre>
     * public static double mean(double... values)
     * </pre>
     *
     * @param m the method to test
     * @return true if the method takes a variable number of arguments.
     */
    public static boolean isVarArgs(Method m) {
        //if ("1.5".compareTo(SysProperties.JAVA_SPECIFICATION_VERSION) > 0) {
        //    return false;
        //}
        try {
            Method isVarArgs = m.getClass().getMethod("isVarArgs", new Class[0]);
            Boolean result = (Boolean) isVarArgs.invoke(m, new Object[0]);
            return result.booleanValue();
        } catch (Exception e) {
            return false;
        }
    }

}
