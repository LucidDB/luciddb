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
package org.eigenbase.applib.mondrian;

import java.net.*;


/**
 * Implements a simple, Mondrian cache clearing call to a Pentaho server
 *
 * @author Nicholas Goodman
 * @version $Id$
 */
public abstract class ClearPentahoMondrianCacheUdp
{
    //~ Static fields/initializers ---------------------------------------------

    private static String URL_TEMPLATE =
        "<<server url>>ViewAction?solution=admin&path=&action=clear_mondrian_schema_cache.xaction&userid=<<username>>&password=<<password>>";
    private static String DEFAULT_USER_NAME = "joe";
    private static String DEFAULT_PASSWORD = "password";

    //~ Methods ----------------------------------------------------------------

    public static void execute(
        String baseServerUrl,
        String userName,
        String password)
        throws Exception
    {
        String userReplacement = DEFAULT_USER_NAME;
        String passwordReplacement = DEFAULT_PASSWORD;

        // Check values
        if (userName != null) {
            userReplacement = userName;
        }
        if (password != null) {
            passwordReplacement = password;
        }

        // Replace Values in template
        String finalUrlString =
            URL_TEMPLATE.replace(
                "<<server url>>",
                baseServerUrl).replace("<<username>>", userReplacement).replace(
                "<<password>>",
                passwordReplacement);

        // HIT URL
        HttpURLConnection httpConn = null;
        try {
            URL url = new URL(finalUrlString);
            httpConn = (HttpURLConnection) url.openConnection();
            if (httpConn.getResponseCode() != 200) {
                throw new Exception(
                    "URL is not 200:" + finalUrlString
                    + httpConn.getResponseMessage());
            }
        } finally {
            if (httpConn != null) {
                httpConn.disconnect();
            }
        }
    }
}

// End ClearPentahoMondrianCacheUdp.java
