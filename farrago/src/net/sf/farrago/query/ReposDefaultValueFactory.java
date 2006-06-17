/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import org.eigenbase.sql2rel.DefaultValueFactory;
import org.eigenbase.rex.*;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.util.Util;
import net.sf.farrago.util.FarragoObjectCache;
import net.sf.farrago.util.FarragoUtil;
import net.sf.farrago.cwm.relational.CwmColumn;
import net.sf.farrago.cwm.relational.CwmSqldataType;
import net.sf.farrago.cwm.core.CwmExpression;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.session.FarragoSessionParser;

import java.util.Map;
import java.util.HashMap;

/**
 * DefaultValueFactory looks up a default
 * value stored in the catalog, parses it, and converts it to an
 * Expression.  Processed expressions are cached for use by subsequent
 * calls.  The CwmExpression's MofId is used as the cache key.
 */
public class ReposDefaultValueFactory implements DefaultValueFactory,
    FarragoObjectCache.CachedObjectFactory
{
    private Map constructorToSqlMap = new HashMap();
    protected FarragoPreparingStmt farragoPreparingStmt;

    public ReposDefaultValueFactory(FarragoPreparingStmt farragoPreparingStmt)
    {
        this.farragoPreparingStmt = farragoPreparingStmt;
    }

    // implement DefaultValueFactory
    public boolean isGeneratedAlways(
        RelOptTable table,
        int iColumn)
    {
        if (table instanceof FarragoQueryColumnSet) {
            FarragoQueryColumnSet queryColumnSet =
                (FarragoQueryColumnSet) table;
            CwmColumn column = 
                (CwmColumn) queryColumnSet.getCwmColumnSet()
                .getFeature().get(iColumn);
            if (column instanceof FemStoredColumn) {
                return ((FemStoredColumn) column).isGeneratedAlways(); 
            }
        }
        return false;
    }

    // implement DefaultValueFactory
    public RexNode newColumnDefaultValue(
        RelOptTable table,
        int iColumn)
    {
        if (!(table instanceof FarragoQueryColumnSet)) {
            return farragoPreparingStmt.sqlToRelConverter.getRexBuilder().constantNull();
        }
        FarragoQueryColumnSet queryColumnSet =
            (FarragoQueryColumnSet) table;
        CwmColumn column =
            (CwmColumn) queryColumnSet.getCwmColumnSet().getFeature().get(iColumn);
        if (column instanceof FemStoredColumn) {
            FemStoredColumn storedColumn = (FemStoredColumn) column;
            FemSequenceGenerator sequence = storedColumn.getSequence();
            if (sequence != null) {
                return sequenceValue(sequence);
            }
        }
        return convertExpression(column.getInitialValue());
    }

    // dimplement DefaultValueFactory
    public RexNode newAttributeInitializer(
        RelDataType type,
        SqlFunction constructor,
        int iAttribute,
        RexNode [] constructorArgs)
    {
        SqlIdentifier typeName = type.getSqlIdentifier();
        CwmSqldataType cwmType = farragoPreparingStmt.getStmtValidator().findSqldataType(typeName);
        assert(cwmType instanceof FemSqlobjectType);
        FemSqltypeAttribute attribute =
            (FemSqltypeAttribute) cwmType.getFeature().get(iAttribute);
        if (constructor instanceof FarragoUserDefinedRoutine) {
            RexNode initializer = convertConstructorAssignment(
                (FarragoUserDefinedRoutine) constructor,
                attribute,
                constructorArgs);
            if (initializer != null) {
                return initializer;
            }
        }
        return convertExpression(attribute.getInitialValue());
    }

    private RexNode convertConstructorAssignment(
        FarragoUserDefinedRoutine constructor,
        FemSqltypeAttribute attribute,
        RexNode [] constructorArgs)
    {
        SqlNodeList nodeList = (SqlNodeList)
            constructorToSqlMap.get(constructor.getFemRoutine());
        if (nodeList == null) {
            assert (constructor.hasDefinition());
            FarragoSessionParser parser =
                farragoPreparingStmt.getSession().getPersonality().newParser(
                    farragoPreparingStmt.getSession());
            String body = constructor.getFemRoutine().getBody().getBody();
            nodeList = (SqlNodeList) parser.parseSqlText(
                farragoPreparingStmt.getStmtValidator(),
                null,
                body,
                true);
            constructorToSqlMap.put(constructor.getFemRoutine(), nodeList);
        }
        SqlNode rhs = null;
        for (SqlNode node : nodeList) {
            SqlCall call = (SqlCall) node;
            SqlIdentifier lhs = (SqlIdentifier) call.getOperands()[0];
            if (lhs.getSimple().equals(attribute.getName())) {
                rhs = call.getOperands()[1];
                break;
            }
        }
        if (rhs == null) {
            return null;
        }
        FarragoRoutineInvocation invocation = new FarragoRoutineInvocation(
            constructor,
            constructorArgs);
        return farragoPreparingStmt.expandInvocationExpression(rhs, invocation);
    }

    private RexNode convertExpression(CwmExpression cwmExp)
    {
        if (cwmExp.getBody().equalsIgnoreCase("NULL")) {
            return farragoPreparingStmt.sqlToRelConverter.getRexBuilder().constantNull();
        }

        FarragoObjectCache.Entry cacheEntry =
            farragoPreparingStmt.getStmtValidator().getCodeCache().pin(
                cwmExp.refMofId(),
                this,
                false);
        RexNode parsedExp = (RexNode) cacheEntry.getValue();
        farragoPreparingStmt.getStmtValidator().getCodeCache().unpin(cacheEntry);
        return parsedExp;
    }

    private RexNode sequenceValue(FemSequenceGenerator sequence)
    {
        RexBuilder rexBuilder =
            farragoPreparingStmt.sqlToRelConverter.getRexBuilder();
        return rexBuilder.makeCall(
            SqlStdOperatorTable.nextValueFunc,
            rexBuilder.makeLiteral(sequence.refMofId()));
    }

    // implement CachedObjectFactory
    public void initializeEntry(
        Object key,
        FarragoObjectCache.UninitializedEntry entry)
    {
        String mofId = (String) key;
        CwmExpression cwmExp =
            (CwmExpression) farragoPreparingStmt.getRepos().getMdrRepos().getByMofId(mofId);
        String defaultString = cwmExp.getBody();
        SqlParser sqlParser = new SqlParser(defaultString);
        SqlNode sqlNode;
        try {
            sqlNode = sqlParser.parseExpression();
        } catch (SqlParseException ex) {
            // parsing of expressions already stored in the catalog should
            // always succeed
            throw Util.newInternal(ex);
        }
        RexNode exp = farragoPreparingStmt.sqlToRelConverter.convertExpression(sqlNode);

        // TODO:  better memory usage estimate
        entry.initialize(exp,
            3 * FarragoUtil.getStringMemoryUsage(defaultString));
    }
}
