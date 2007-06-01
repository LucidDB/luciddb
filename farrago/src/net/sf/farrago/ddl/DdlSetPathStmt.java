/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
