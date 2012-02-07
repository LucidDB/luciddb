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
package net.sf.farrago.ddl;

import java.util.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


/**
 * DdlSetPathStmt represents a statement (SET PATH) that establishes a default
 * SQL-path.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlSetPathStmt
    extends DdlSetContextStmt
{
    //~ Instance fields --------------------------------------------------------

    private List<SqlIdentifier> schemaList;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlSetPathStmt.
     *
     * @param valueExpr value expression for new SQL-path
     */
    public DdlSetPathStmt(SqlNode valueExpr)
    {
        super(valueExpr);
    }

    //~ Methods ----------------------------------------------------------------

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    // implement DdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        super.preValidate(ddlValidator);
        List<SqlNode> schemaExprList = null;
        if (parsedExpr instanceof SqlIdentifier) {
            schemaExprList = Collections.singletonList(parsedExpr);
        } else if (parsedExpr instanceof SqlCall) {
            SqlCall call = (SqlCall) parsedExpr;
            if (call.getOperator().getName().equalsIgnoreCase("row")) {
                schemaExprList = Arrays.asList(call.getOperands());
            }
        }
        if (schemaExprList != null) {
            this.schemaList = new ArrayList<SqlIdentifier>();
            for (SqlNode obj : schemaExprList) {
                if (!(obj instanceof SqlIdentifier)) {
                    schemaExprList = null;
                    break;
                }
                SqlIdentifier id = (SqlIdentifier) obj;
                if ((id.names.length < 1) || (id.names.length > 2)) {
                    schemaExprList = null;
                    break;
                }
                if (id.isSimple()) {
                    // fully qualify schema names
                    FarragoSessionVariables sessionVariables =
                        ddlValidator.getStmtValidator().getSessionVariables();
                    String [] names =
                    { sessionVariables.catalogName, id.getSimple() };
                    SqlParserPos [] poses =
                    { SqlParserPos.ZERO, id.getParserPosition() };
                    id.setNames(names, poses);
                }
                schemaList.add(id);
            }
        }
        if (schemaExprList == null) {
            this.schemaList = null;
            throw FarragoResource.instance().ValidatorSetPathInvalidExpr.ex(
                ddlValidator.getRepos().getLocalizedObjectName(valueString));
        }
    }

    public List<SqlIdentifier> getSchemaList()
    {
        return schemaList;
    }
}

// End DdlSetPathStmt.java
