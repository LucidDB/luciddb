/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2010 DynamoBI Corporation
// Copyright (C) 2007-2007 The Eigenbase Project
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
package com.lucidera.luciddb.applib.mondrian;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Implements a simple, Mondrian cache clearing call to a Pentaho server
 * 
 * @author Nicholas Goodman
 * @version $Id$
 */
public abstract class ClearPentahoMondrianCacheUdp
{
    private static String URLTemplate = "<<server url>>ViewAction?solution=admin&path=&action=clear_mondrian_schema_cache.xaction&userid=<<username>>&password=<<password>>";
    private static String defaultUserName = "joe";
    private static String defaultPassword = "password";

    public static void execute(
        String base_server_url,
        String username,
        String password)
        throws Exception
    {

        String userreplacement = defaultUserName;
        String passwordreplacement = defaultPassword;

        // Check values
        if (username != null)
            userreplacement = username;
        if (password != null)
            passwordreplacement = password;

        // Replace Values in template
        String URLString = URLTemplate.replace(
            "<<server url>>",
            base_server_url).replace("<<username>>", userreplacement).replace(
            "<<password>>",
            passwordreplacement);

        // HIT URL
        URL url = new URL(URLString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        if (conn.getResponseCode() != 200)
            throw new Exception("ERROR CODE from URL:" + URLString);

    }
}

// End ClearPentahoMondrianCacheUdp.java
