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
package org.luciddb.session;

import net.sf.farrago.query.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.validate.*;


/**
 * LucidDbPreparingStmt refines {@link FarragoPreparingStmt} with
 * LucidDB-specifics.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbPreparingStmt
    extends FarragoPreparingStmt
{
    //~ Constructors -----------------------------------------------------------

    public LucidDbPreparingStmt(
        FarragoSessionStmtContext rootStmtContext,
        FarragoSessionStmtValidator stmtValidator,
        String sql)
    {
        super(rootStmtContext, stmtValidator, sql);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionPreparingStmt
    public SqlValidator getSqlValidator()
    {
        if (sqlValidator == null) {
            sqlValidator =
                new LucidDbSqlValidator(
                    this,
                    SqlConformance.Default);
        }
        return sqlValidator;
    }
}

// End LucidDbPreparingStmt.java
