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
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.session.*;

import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql2rel.*;


/**
 * ScalarSubqueryConverter converts subqueries to scalar constants by evaulating
 * them and passing back the resulting constant expression. By doing so, this
 * means that the statement containing the subquery can no longer be cached.
 *
 * <p>This class can also be used to convert EXISTS subqueries by replacing the
 * EXISTS with a boolean value indicating whether the subquery returns zero
 * (FALSE) or at least one (TRUE) row.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class ScalarSubqueryConverter
    implements SubqueryConverter
{
    //~ Instance fields --------------------------------------------------------

    private final FarragoSessionPreparingStmt stmt;

    //~ Constructors -----------------------------------------------------------

    public ScalarSubqueryConverter(FarragoSessionPreparingStmt stmt)
    {
        this.stmt = stmt;
    }

    //~ Methods ----------------------------------------------------------------

    // implement SubqueryConverter
    public boolean canConvertSubquery()
    {
        return true;
    }

    // implement SubqueryConverter
    public RexNode convertSubquery(
        SqlCall subquery,
        SqlToRelConverter parentConverter,
        boolean isExists,
        boolean isExplain)
    {
        // Use a FarragoReentrantSubquery to evaluate the subquery
        List<RexNode> reducedValues = new ArrayList<RexNode>();
        FarragoReentrantSubquery reentrantStmt =
            new FarragoReentrantSubquery(
                subquery,
                parentConverter,
                isExists,
                isExplain,
                reducedValues);
        reentrantStmt.execute(stmt.getSession(), true);
        if (reentrantStmt.failed) {
            return null;
        } else {
            stmt.disableStatementCaching();
            return reducedValues.get(0);
        }
    }
}

// End ScalarSubqueryConverter.java
