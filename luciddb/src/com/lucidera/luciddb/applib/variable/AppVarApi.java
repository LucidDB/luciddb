/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
package com.lucidera.luciddb.applib.variable;

/**
 * AppVarApi defineds UDR entry points for managing application variables.
 * See DDL in luciddb/initsql/installApplib.sql.
 *
 * @author John Sichi
 * @version $Id$
 */
public abstract class AppVarApi
{
    public static void executeCreate(
        String contextId, String varId, String description)
    {
        CreateAppVarUdp.execute(contextId, varId, description);
    }
    
    public static void executeDelete(
        String contextId, String varId)
    {
        DeleteAppVarUdp.execute(contextId, varId);
    }

    public static void executeFlush(
        String contextId, String varId)
    {
        FlushAppVarUdp.execute(contextId, varId);
    }

    public static void executeSet(
        String contextId, String varId, String newValue)
    {
        SetAppVarUdp.execute(contextId, varId, newValue);
    }

    public static String executeGet(
        String contextId, String varId)
    {
        return GetAppVarUdf.execute(contextId, varId);
    }
}

// End AppVarApi.java
