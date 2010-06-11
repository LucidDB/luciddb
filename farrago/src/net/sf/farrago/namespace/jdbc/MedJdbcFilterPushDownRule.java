/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 Dynamo BI Corporation
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
package net.sf.farrago.namespace.jdbc;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * MedJdbcFilterPushDownRule is a rule to push filer down into
 * JDBC sources (wrapping rather than rewriting the foreign SQL; for
 * filter/projection rewrite, see {@link MedJdbcPushDownRule}).
 *
 * @author John Sichi
 * @version $Id$
 */
public class MedJdbcFilterPushDownRule
    extends RelOptRule
{
    public static final MedJdbcFilterPushDownRule instance =
        new MedJdbcFilterPushDownRule();

    public MedJdbcFilterPushDownRule()
    {
        super(
            new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand(MedJdbcQueryRel.class, ANY)));
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FilterRel filterRel = (FilterRel) call.rels[0];
        final MedJdbcQueryRel queryRel = (MedJdbcQueryRel) call.rels[1];

        RexToSqlNodeConverter exprConverter =
            new RexToSqlNodeConverterImpl(
                new RexSqlStandardConvertletTable())
            {
                public SqlIdentifier convertInputRef(RexInputRef ref)
                {
                    RelDataType fields = queryRel.getRowType();
                    return new SqlIdentifier(
                        fields.getFieldList().get(ref.getIndex()).getName(),
                        SqlParserPos.ZERO);
                }
            };

        SqlNode whereClause =
            exprConverter.convertNode(filterRel.getCondition());
        if (whereClause == null) {
            // could not translate
            return;
        }

        SqlSelect selectWithFilter =
            SqlStdOperatorTable.selectOperator.createCall(
                null,
                new SqlNodeList(
                    Collections.singletonList(
                        new SqlIdentifier("*", SqlParserPos.ZERO)),
                    SqlParserPos.ZERO),
                queryRel.getSql(),
                whereClause,
                null,
                null,
                null,
                null,
                SqlParserPos.ZERO);
        if (!queryRel.getServer().isRemoteSqlValid(selectWithFilter)) {
            return;
        }

        RelNode rel =
            new MedJdbcQueryRel(
                queryRel.getServer(),
                queryRel.getColumnSet(),
                queryRel.getCluster(),
                filterRel.getRowType(),
                queryRel.getConnection(),
                queryRel.getDialect(),
                selectWithFilter,
                null);
        call.transformTo(rel);
    }
}

// End MedJdbcFilterPushDownRule.java
