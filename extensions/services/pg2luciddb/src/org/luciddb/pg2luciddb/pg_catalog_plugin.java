/*
// $Id$
// pg2luciddb is a PG emulator for LucidDB
// Copyright (C) 2009 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2009 Dynamo BI Corporation
// Portions Copyright (C) 2009 Alexander Mekhrishvili
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
*/

package org.luciddb.pg2luciddb;

import java.util.*;
import java.io.*;
import java.sql.*;

public class pg_catalog_plugin
{
    // function to convert mofId to integer value:
    public static Integer mofIdToInteger(String input)
    {
        if (input.startsWith("j:"))
            input = input.substring(2);

        return Integer.parseInt(input, 16);
    }

    // get user by id:
    public static String getUserById(int id) throws SQLException 
    {
        String res = null;

        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement prep = conn.prepareStatement("SELECT USENAME FROM PG_CATALOG.PG_USER WHERE OID=?");
        prep.setInt(1, id);
        ResultSet rs = prep.executeQuery();
        if (rs.next()) 
        {
            res = rs.getString(1);
        }
        rs.close();
        return res;
    }

    // dummy procedure:
    public static void dummyProcedure(String input)
    {
    }
}

