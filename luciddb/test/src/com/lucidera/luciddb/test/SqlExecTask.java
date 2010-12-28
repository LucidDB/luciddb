/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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

package com.lucidera.luciddb.test;

import java.util.Properties;

import org.eigenbase.blackhawk.core.AntProperties;
import org.eigenbase.blackhawk.core.test.*;
import org.eigenbase.blackhawk.extension.exectask.junit.*;

/**
 * Uses JUnitRunner to run a junit test.
 **/
public class SqlExecTask extends JunitExecTask
{

    String sqlfile = null;

    public TestLogicTask getTestLogicTask()
    {

        Properties props = getParameters().getAll();
        props.setProperty("sql-file", this.sqlfile);
        BasicInternalParameters params = new BasicInternalParameters(props);

        return new JunitTestLogicTask(
            getTestClassName(),
            params,
            getResultHandler(),
            getMethodNamesToRun(),
            AntProperties.isJunitValidateMethodNamesEnabled(),
            AntProperties.isJunitLogOnlyFailureEnabled());
    }

    public void setFile(String in)
    {
        testClassName = "com.lucidera.luciddb.test.SqlTest";
        this.sqlfile = in;
    }

}
