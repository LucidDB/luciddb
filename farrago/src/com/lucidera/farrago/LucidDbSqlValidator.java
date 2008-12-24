/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
package com.lucidera.farrago;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;


/**
 * LucidDbSqlValidator refines {@link FarragoSqlValidator} with
 * LucidDB-specifics.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LucidDbSqlValidator
    extends FarragoSqlValidator
{
    //~ Constructors -----------------------------------------------------------

    public LucidDbSqlValidator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        SqlConformance conformance,
        FarragoPreparingStmt preparingStmt)
    {
        super(
            opTab,
            catalogReader,
            typeFactory,
            conformance,
            preparingStmt);
    }

    public LucidDbSqlValidator(
        FarragoPreparingStmt preparingStmt,
        SqlConformance conformance)
    {
        super(preparingStmt, conformance);
    }

    //~ Methods ----------------------------------------------------------------

    // override FarragoSqlValidator
    public void validateMerge(SqlMerge call)
    {
        super.validateMerge(call);

        // updates on unique keys only allowed in fail-fast mode
        if (!inFailFastMode()) {
            validateMergeUniqueKeys(call);
        }
    }

    /**
     * Validates the merge statement for updates on unique key columns
     *
     * @param call merge statement
     */
    private void validateMergeUniqueKeys(SqlMerge call)
    {
        SqlUpdate updateCall = call.getUpdateCall();
        if (updateCall == null) {
            return;
        }

        // retrieve a bitmap containing any column that is part of a unique
        // key
        SqlValidatorNamespace targetNs = getNamespace(call);
        RelOptTable targetTable =
            SqlValidatorUtil.getRelOptTable(
                targetNs,
                getPreparingStmt().getRelOptSchema(),
                null,
                null);
        BitSet uniqueCols =
            FarragoCatalogUtil.getUniqueKeyCols(
                ((MedAbstractColumnSet) targetTable).getCwmColumnSet());

        SqlNodeList updateList = updateCall.getTargetColumnList();
        SqlSelect sourceSelect = updateCall.getSourceSelect();
        SqlNodeList selectList = sourceSelect.getSelectList();

        // map the target columns to their corresponding column number
        int numTableCols = selectList.size() - updateList.size();
        Map<String, Integer> columnMap = new HashMap<String, Integer>();
        int columnNum = 0;
        for (SqlNode col : selectList) {
            SqlIdentifier id = (SqlIdentifier) col;
            String name = id.names[id.names.length - 1];
            columnMap.put(name, columnNum++);
            if (columnNum == numTableCols) {
                break;
            }
        }

        // determine the name used to qualify the target table
        SqlIdentifier table = call.getAlias();
        String tableName;
        if (table == null) {
            String [] names = targetTable.getQualifiedName();
            tableName = names[names.length - 1];
        } else {
            tableName = table.getSimple();
        }

        // using the name to column number map created above, determine if
        // the update column is a unique key; if it is, validate the column
        SqlNodeList updateExprsList = updateCall.getSourceExpressionList();
        Iterator<SqlNode> updateExprIter = updateExprsList.iterator();
        for (SqlNode updateCol : updateList) {
            SqlIdentifier col = (SqlIdentifier) updateCol;
            SqlNode updateExpr = updateExprIter.next();
            String columnName = col.getSimple();
            columnNum = columnMap.get(columnName);
            if (uniqueCols.get(columnNum)) {
                // fully qualify the target column name to simplify name
                // matching
                final SqlParserPos pos = col.getParserPosition();
                SqlIdentifier uCol =
                    new SqlIdentifier(
                        new String[] { tableName, columnName },
                        null,
                        pos,
                        new SqlParserPos[] {
                            SqlParserPos.ZERO,
                            pos
                        });
                if (!validateUniqueColumn(
                        updateExpr,
                        call.getCondition(),
                        uCol,
                        getWhereScope(sourceSelect)))
                {
                    throw FarragoResource.instance().ValidatorUniqueKeyUpdate
                    .ex(
                        columnName);
                }
            }
        }
    }

    /**
     * Validates a unique column to ensure that either it isn't being updated,
     * or if it's being updated, the update is a no-op because the column is
     * either being set to itself or the column it is being equi-joined with in
     * the ON clause of the MERGE statement
     *
     * @param sourceExpr expression that the column is being updated with
     * @param onCondition ON condition of the MERGE statement
     * @param targetCol target column being updated
     * @param scope scope of the body of the statement
     *
     * @return true if the unique column is not updated or is updated to a value
     * that does not change the column value
     */
    private boolean validateUniqueColumn(
        SqlNode sourceExpr,
        SqlNode onCondition,
        SqlIdentifier targetCol,
        SqlValidatorScope scope)
    {
        // allow a dummy cast on the source expression
        if (sourceExpr.isA(SqlKind.Cast)) {
            SqlCall cast = (SqlCall) sourceExpr;
            RelDataType castType = getValidatedNodeType(sourceExpr);
            RelDataType sourceType =
                getValidatedNodeType(cast.getOperands()[0]);

            // cast result will always be nullable, so underlying
            // expressions as nullable for comparison purposes
            sourceType =
                typeFactory.createTypeWithNullability(
                    sourceType,
                    true);
            if (castType != sourceType) {
                return false;
            }
            sourceExpr = cast.getOperands()[0];
        }

        // check if the update expression is a simple identifier
        if (sourceExpr.getKind() != SqlKind.Identifier) {
            return false;
        }

        // if it is a simple identifier, make sure the update is a no-op by
        // ensuring that the update expression is either the column itself or
        // the column the update key joins with in the ON condition
        SqlIdentifier sourceCol = (SqlIdentifier) expand(sourceExpr, scope);
        if (targetCol.equalsDeep(sourceCol, false)) {
            return true;
        }
        EquiJoinFinder equiJoinFinder =
            new EquiJoinFinder(targetCol, sourceCol, scope);
        onCondition.accept(equiJoinFinder);
        return equiJoinFinder.foundMatch();
    }

    private boolean inFailFastMode()
    {
        FarragoSessionVariables vars =
            getPreparingStmt().getSession().getSessionVariables();
        Integer errorMax = vars.getInteger(LucidDbSessionPersonality.ERROR_MAX);
        return ((errorMax != null) && (errorMax == 0));
    }

    // override SqlValidatorImpl
    protected SqlNode getSelfJoinExprForUpdate(
        SqlIdentifier table,
        String alias)
    {
        // For LucidDB, when rewriting UPDATE to MERGE, we
        // generate a self-join of the form
        //     LCS_RID(src.x) = LCS_RID(tgt.y).

        // For example, given
        //
        // create table x.t(i int, j int);
        // update x.t set i = i + 1, j = 7 where j > 10;
        //
        // The rewrite produces
        //
        // MERGE INTO "LOCALDB"."X"."T" AS "SYS$TGT"
        // USING (SELECT "I" AS "SYS$ANON1", "J" AS "SYS$ANON2"
        //        FROM "X"."T") AS "SYS$SRC"
        // ON LCS_RID("SYS$SRC"."SYS$ANON1") = LCS_RID("SYS$TGT"."I")
        // AND "J" > 10
        // WHEN MATCHED THEN UPDATE SET "I" = "I" + 1, "J" = 7
        
        // LCS_RID doesn't care which column we choose (only which table
        // reference it comes from), so we can arbitrarily pick the first
        // column in the table.
        String colName;
        if (alias.equals(UPDATE_SRC_ALIAS)) {
            // The source columns have been anonymized, so
            // we reference the first one by position as
            // "SYS$ANON1"
            colName = UPDATE_ANON_PREFIX + "1";
        } else {
            // The target columns retained their names, so look
            // up the table and get the name of the first column.
            RelOptTable relOptTable = getPreparingStmt().loadColumnSet(table);
            if (relOptTable == null) {
                // let validator complain about non-existent table
                return null;
            }
            colName =
                relOptTable.getRowType().getFieldList().get(0).getName();
        }
        SqlNode colRef = null;
        SqlIdentifier colId = new SqlIdentifier(
            new String [] { alias, colName }, SqlParserPos.ZERO);
        SqlNode lcsRidCall = LucidDbOperatorTable.lcsRidFunc.createCall(
            SqlParserPos.ZERO,
            colId);
        return lcsRidCall;
    }
    
    //~ Inner Classes ----------------------------------------------------------

    /**
     * Visitor that walks a SqlNode expression tree looking for an equijoin
     * between a source and target column.
     */
    private class EquiJoinFinder
        extends SqlBasicVisitor<Void>
    {
        private SqlIdentifier targetCol;
        private SqlIdentifier sourceCol;
        private SqlValidatorScope scope;
        private boolean found;

        EquiJoinFinder(
            SqlIdentifier targetCol,
            SqlIdentifier sourceCol,
            SqlValidatorScope scope)
        {
            this.targetCol = targetCol;
            this.sourceCol = sourceCol;
            this.scope = scope;
        }

        public Void visit(SqlCall call)
        {
            // if an equijoin is found, either look for an expression of the
            // form "targetCol = sourceCol" or "sourceCol = targetCol"
            if (call.getOperator() == SqlStdOperatorTable.equalsOperator) {
                SqlNode leftOp = call.getOperands()[0];
                SqlNode rightOp = call.getOperands()[1];
                if (checkOperands(leftOp, rightOp)) {
                    return null;
                }
                if (checkOperands(rightOp, leftOp)) {
                    return null;
                }
            } else if (call.getOperator() == SqlStdOperatorTable.andOperator) {
                return super.visit(call);
            }
            return null;
        }

        /**
         * Determines if the first operand matches the target column and the
         * second matches the source column
         *
         * @param op1 first operand
         * @param op2 second operand
         *
         * @return true if a match has been found
         */
        private boolean checkOperands(SqlNode op1, SqlNode op2)
        {
            if (op1.getKind() == SqlKind.Identifier) {
                SqlIdentifier col1 = (SqlIdentifier) expand(op1, scope);
                if (col1.equalsDeep(targetCol, false)) {
                    if (op2.getKind() == SqlKind.Identifier) {
                        SqlIdentifier col2 = (SqlIdentifier) expand(op2, scope);
                        if (col2.equalsDeep(sourceCol, false)) {
                            found = true;
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        public boolean foundMatch()
        {
            return found;
        }
    }
}

// End LucidDbSqlValidator.java
