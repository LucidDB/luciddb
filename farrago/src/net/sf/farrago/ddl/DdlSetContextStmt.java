/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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

import java.sql.*;

import java.util.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.pretty.*;
import org.eigenbase.sql.type.*;


/**
 * DdlSetContextStmt is an abstract base class for DDL statements (such as SET
 * SCHEMA) which modify context variables.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class DdlSetContextStmt
    extends DdlStmt
{
    //~ Instance fields --------------------------------------------------------

    private final SqlNode valueExpr;

    protected String valueString;

    protected SqlNode parsedExpr;

    //~ Constructors -----------------------------------------------------------

    protected DdlSetContextStmt(SqlNode valueExpr)
    {
        super(null);
        this.valueExpr = valueExpr;
    }

    //~ Methods ----------------------------------------------------------------

    // override DdlStmt
    public boolean requiresCommit()
    {
        return false;
    }

    public SqlNode getValueExpression()
    {
        return valueExpr;
    }

    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        // Use a reentrant session to simplify cleanup.
        FarragoSession session = ddlValidator.newReentrantSession();
        try {
            processExpression(session);
        } catch (Throwable ex) {
            throw FarragoResource.instance().ValidatorSetStmtInvalid.ex(ex);
        } finally {
            ddlValidator.releaseReentrantSession(session);
        }
    }

    private void processExpression(FarragoSession session)
        throws Exception
    {
        // We're given a string value expression.  First step is to
        // evaluate it to a string value.  We do this by executing
        // the query "VALUES <valueExpr>".

        SqlDialect dialect = SqlDialect.create(session.getDatabaseMetaData());
        SqlPrettyWriter writer = new SqlPrettyWriter(dialect);
        writer.keyword("VALUES");
        valueExpr.unparse(writer, 0, 0);

        String sql = writer.toString();

        // null param def factory okay because the SQL does not use dynamic
        // parameters
        FarragoSessionStmtContext stmtContext = session.newStmtContext(null);
        stmtContext.prepare(sql, true);
        RelDataType rowType = stmtContext.getPreparedRowType();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        if (fieldList.size() == 1) {
            RelDataType type = fieldList.get(0).getType();
            if (!SqlTypeUtil.inCharFamily(type)) {
                fieldList = null;
            }
        } else {
            fieldList = null;
        }
        if (fieldList == null) {
            // expression is not a simple string value
            throw FarragoResource.instance().ValidatorSetStmtNonString.ex();
        }
        stmtContext.execute();
        ResultSet resultSet = stmtContext.getResultSet();
        boolean gotRow = resultSet.next();
        assert (gotRow);
        valueString = resultSet.getString(1);
        resultSet.close();

        // Now we have the string value.  Oddly enough, the next thing
        // we're supposed to do is go back and parse it into another
        // expression.  Our subclass will decide what to do with it.
        // We use a SqlParser instead of a FarragoSessionParser because
        // we don't expect anything more than a list of identifiers.

        SqlParser parser = new SqlParser("(" + valueString + ")");
        parsedExpr = parser.parseExpression();
    }
}

// End DdlSetContextStmt.java
