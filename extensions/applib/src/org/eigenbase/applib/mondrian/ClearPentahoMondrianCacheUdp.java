/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
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
