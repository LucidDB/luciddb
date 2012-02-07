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

import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.*;


/**
 * DdlSetCatalogStmt represents the DDL statement SET CATALOG.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlSetCatalogStmt
    extends DdlSetContextStmt
{
    //~ Instance fields --------------------------------------------------------

    private SqlIdentifier catalogName;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlSetCatalogStmt.
     *
     * @param valueExpr value expression for new catalog
     */
    public DdlSetCatalogStmt(SqlNode valueExpr)
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
        if (parsedExpr instanceof SqlIdentifier) {
            catalogName = (SqlIdentifier) parsedExpr;
            if (!catalogName.isSimple()) {
                catalogName = null;
            }
        }
        if (catalogName == null) {
            throw FarragoResource.instance().ValidatorSetCatalogInvalidExpr.ex(
                ddlValidator.getRepos().getLocalizedObjectName(valueString));
        }
    }

    public SqlIdentifier getCatalogName()
    {
        return catalogName;
    }
}

// End DdlSetCatalogStmt.java
