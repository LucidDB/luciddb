/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 DynamoBI Corporation
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
package org.eigenbase.applib.variable;

/**
 * AppVarApi defineds UDR entry points for managing application variables. See
 * DDL in luciddb/initsql/installApplib.sql.
 *
 * @author John Sichi
 * @version $Id$
 */
public abstract class AppVarApi
{
    //~ Methods ----------------------------------------------------------------

    public static void executeCreate(
        String contextId,
        String varId,
        String description)
    {
        CreateAppVarUdp.execute(contextId, varId, description);
    }

    public static void executeDelete(
        String contextId,
        String varId)
    {
        DeleteAppVarUdp.execute(contextId, varId);
    }

    public static void executeFlush(
        String contextId,
        String varId)
    {
        FlushAppVarUdp.execute(contextId, varId);
    }

    public static void executeSet(
        String contextId,
        String varId,
        String newValue)
    {
        SetAppVarUdp.execute(contextId, varId, newValue);
    }

    public static String executeGet(
        String contextId,
        String varId)
    {
        return GetAppVarUdf.execute(contextId, varId);
    }
}

// End AppVarApi.java
