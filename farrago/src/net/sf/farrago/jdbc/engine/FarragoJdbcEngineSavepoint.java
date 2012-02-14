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
package net.sf.farrago.jdbc.engine;

import java.sql.*;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.session.*;


/**
 * FarragoJdbcEngineSavepoint implements the {@link java.sql.Savepoint}
 * interface for Farrago.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcEngineSavepoint
    implements Savepoint
{
    //~ Instance fields --------------------------------------------------------

    FarragoSessionSavepoint farragoSavepoint;

    //~ Constructors -----------------------------------------------------------

    FarragoJdbcEngineSavepoint(FarragoSessionSavepoint farragoSavepoint)
    {
        this.farragoSavepoint = farragoSavepoint;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Savepoint
    public int getSavepointId()
        throws SQLException
    {
        if (farragoSavepoint.getName() != null) {
            throw new SQLException("Can't getSavepointId for named Savepoint");
        }
        return farragoSavepoint.getId();
    }

    // implement Savepoint
    public String getSavepointName()
        throws SQLException
    {
        if (farragoSavepoint.getName() == null) {
            throw new SQLException(
                "Can't getSavepointName for unnamed Savepoint");
        }
        return farragoSavepoint.getName();
    }
}

// End FarragoJdbcEngineSavepoint.java
