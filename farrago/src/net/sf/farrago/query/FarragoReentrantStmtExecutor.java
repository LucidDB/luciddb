/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 SQLstream, Inc.
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package net.sf.farrago.query;

import java.math.*;

import java.sql.*;

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;


/**
 * FarragoReentrantStmtExecutor extends {@link FarragoReentrantStmt} by
 * providing a base method for executing a query plan and returning its result.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoReentrantStmtExecutor
    extends FarragoReentrantStmt
{
    // ~ Instance fields -------------------------------------------------------

    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        FarragoTrace.getClassTracer(FarragoReentrantStmtExecutor.class);

    // ~ Constructors ----------------------------------------------------------

    //~ Instance fields --------------------------------------------------------

    protected final RexBuilder rexBuilder;
    protected final List<RexNode> results;
    protected boolean failed;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a FarragoReentrantStmtExecutor.
     *
     * @param rootStmtContext statement context for the root statement of this
     * reentrant statement
     * @param rexBuilder rex builder
     * @param results the resulting evaluated expressions
     */
    FarragoReentrantStmtExecutor(
        FarragoSessionStmtContext rootStmtContext,
        RexBuilder rexBuilder,
        List<RexNode> results)
    {
        super(rootStmtContext);
        this.rexBuilder = rexBuilder;
        this.results = results;
    }

    // ~ Methods ---------------------------------------------------------------

    //~ Methods ----------------------------------------------------------------

    /**
     * Executes a query given a plan containing the corresponding RelNode tree.
     *
     * @param plan the query to be executed
     * @param exprs list of expressions that will be evaluated
     * @param isExists whether the query being executed is part of an EXISTS
     * expression; in this case, the result is either a TRUE or FALSE boolean
     * literal, depending on whether the query returns zero or at least one row
     * @param excludeReduceExprRule true if this execution is already being done
     * to reduce individual expressions
     */
    protected void executePlan(
        RelNode plan,
        List<RexNode> exprs,
        boolean isExists,
        boolean excludeReduceExprRule)
        throws Exception
    {
        // NOTE jvs 26-May-2006: To avoid an infinite loop, we need to
        // make sure the reentrant planner does NOT have
        // FarragoReduceExpressionsRule enabled if this call originated
        // from that rule.
        FarragoPreparingStmt preparingStmt =
            (FarragoPreparingStmt) getPreparingStmt();
        FarragoSessionPlanner reentrantPlanner = preparingStmt.getPlanner();
        if (excludeReduceExprRule) {
            reentrantPlanner.setRuleDescExclusionFilter(
                FarragoReduceExpressionsRule.EXCLUSION_PATTERN);
        }

        getStmtContext().prepare(
            plan,
            SqlKind.Select,
            true,
            getPreparingStmt());
        getStmtContext().execute();
        ResultSet resultSet = getStmtContext().getResultSet();
        if (!resultSet.next()) {
            if (isExists) {
                results.add(rexBuilder.makeLiteral(false));
            } else {
                // This shouldn't happen, but strange things such as
                // error recovery session settings (LER-3372)
                // can surprise us.
                failed = true;
            }
        } else {
            if (isExists) {
                results.add(rexBuilder.makeLiteral(true));
            } else {
                getResultRow(exprs, resultSet);
            }
        }
        resultSet.close();
    }

    private void getResultRow(List<RexNode> exprs, ResultSet resultSet)
        throws Exception
    {
        for (int i = 0; i < exprs.size(); ++i) {
            RexNode expr = exprs.get(i);
            SqlTypeName typeName = expr.getType().getSqlTypeName();
            SqlTypeFamily approxFamily = SqlTypeFamily.APPROXIMATE_NUMERIC;
            double doubleValue = 0.0;
            String stringValue = null;
            if (approxFamily.getTypeNames().contains(typeName)) {
                // Use getDouble to preserve precision.
                doubleValue = resultSet.getDouble(i + 1);
            } else {
                // Anything else can be handled safely via string
                // representation.
                stringValue = resultSet.getString(i + 1);
            }
            RexNode result;
            if (resultSet.wasNull()) {
                result = rexBuilder.constantNull();
                result =
                    rexBuilder.makeCast(
                        expr.getType(),
                        result);
            } else {
                // TODO jvs 26-May-2006:  See comment on RexLiteral
                // constructor regarding SqlTypeFamily.
                typeName = broadenType(typeName);
                RelDataType literalType =
                    rexBuilder.getTypeFactory().createTypeWithNullability(
                        expr.getType(),
                        false);
                if (stringValue == null) {
                    try {
                        result =
                            rexBuilder.makeApproxLiteral(
                                new BigDecimal(doubleValue),
                                literalType);
                    } catch (NumberFormatException ex) {
                        // Infinity or NaN.  For these rare cases,
                        // just skip constant reduction.
                        failed = true;
                        result = null;
                    }
                } else {
                    result =
                        RexLiteral.fromJdbcString(
                            literalType,
                            typeName,
                            stringValue);
                }
            }
            if (tracer.isLoggable(Level.FINE)) {
                tracer.fine(
                    "reduced expression " + expr
                    + " to result " + result);
            }
            results.add(result);
        }

        assert (!resultSet.next());
    }

    // TODO jvs 26-May-2006:  Get rid of this.
    private SqlTypeName broadenType(SqlTypeName typeName)
    {
        if (SqlTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains(
                typeName))
        {
            return SqlTypeName.DOUBLE;
        } else if (
            SqlTypeFamily.EXACT_NUMERIC.getTypeNames().contains(
                typeName))
        {
            return SqlTypeName.DECIMAL;
        } else if (SqlTypeFamily.CHARACTER.getTypeNames().contains(typeName)) {
            return SqlTypeName.CHAR;
        } else if (SqlTypeFamily.BINARY.getTypeNames().contains(typeName)) {
            return SqlTypeName.BINARY;
        } else {
            return typeName;
        }
    }
}

// End FarragoReentrantStmtExecutor.java
