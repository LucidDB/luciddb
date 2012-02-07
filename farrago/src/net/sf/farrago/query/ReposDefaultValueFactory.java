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

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.defimpl.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql2rel.*;
import org.eigenbase.util.*;


/**
 * DefaultValueFactory looks up a default value stored in the catalog, parses
 * it, and converts it to an Expression. Processed expressions are cached for
 * use by subsequent calls. The CwmExpression's MofId is used as the cache key.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ReposDefaultValueFactory
    implements DefaultValueFactory,
        FarragoObjectCache.CachedObjectFactory
{
    //~ Instance fields --------------------------------------------------------

    private Map<FemRoutine, SqlNodeList> constructorToSqlMap =
        new HashMap<FemRoutine, SqlNodeList>();

    protected FarragoPreparingStmt farragoPreparingStmt;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a ReposDefaultValueFactory.
     *
     * @param farragoPreparingStmt Statement preparation context
     */
    public ReposDefaultValueFactory(FarragoPreparingStmt farragoPreparingStmt)
    {
        this.farragoPreparingStmt = farragoPreparingStmt;
    }

    //~ Methods ----------------------------------------------------------------

    // implement DefaultValueFactory
    public boolean isGeneratedAlways(
        RelOptTable table,
        int iColumn)
    {
        FarragoSession session = farragoPreparingStmt.getSession();
        if (session.isReentrantAlterTableRebuild()
            || session.isReentrantAlterTableAddColumn())
        {
            // Override GENERATED ALWAYS for these special reentrant
            // INSERT statements, since we need to be able to reload
            // the existing sequence values.
            return false;
        }

        if (table instanceof FarragoQueryColumnSet) {
            FarragoQueryColumnSet queryColumnSet =
                (FarragoQueryColumnSet) table;
            CwmColumn column =
                (CwmColumn) queryColumnSet.getCwmColumnSet().getFeature().get(
                    iColumn);
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
            return farragoPreparingStmt.sqlToRelConverter.getRexBuilder()
                .constantNull();
        }
        FarragoQueryColumnSet queryColumnSet = (FarragoQueryColumnSet) table;
        CwmColumn column =
            (CwmColumn) queryColumnSet.getCwmColumnSet().getFeature().get(
                iColumn);
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
        CwmSqldataType cwmType =
            farragoPreparingStmt.getStmtValidator().findSqldataType(typeName);
        assert (cwmType instanceof FemSqlobjectType);
        FemSqltypeAttribute attribute =
            (FemSqltypeAttribute) cwmType.getFeature().get(iAttribute);
        if (constructor instanceof FarragoUserDefinedRoutine) {
            RexNode initializer =
                convertConstructorAssignment(
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
        SqlNodeList nodeList =
            constructorToSqlMap.get(constructor.getFemRoutine());
        if (nodeList == null) {
            assert (constructor.hasDefinition());
            FarragoSessionParser parser =
                farragoPreparingStmt.getSession().getPersonality().newParser(
                    farragoPreparingStmt.getSession());
            String body = constructor.getFemRoutine().getBody().getBody();
            nodeList =
                (SqlNodeList) parser.parseSqlText(
                    farragoPreparingStmt.getStmtValidator(),
                    null,
                    body,
                    true);
            constructorToSqlMap.put(
                constructor.getFemRoutine(),
                nodeList);
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
        FarragoRoutineInvocation invocation =
            new FarragoRoutineInvocation(
                constructor,
                constructorArgs);
        return farragoPreparingStmt.expandInvocationExpression(rhs, invocation);
    }

    /**
     * Converts an expression definition from the repository into RexNode
     * format.
     *
     * @param cwmExp Repository object representing an expression
     *
     * @return Rex expression
     */
    private RexNode convertExpression(CwmExpression cwmExp)
    {
        if (null == cwmExp || "NULL".equalsIgnoreCase(cwmExp.getBody())) {
            return farragoPreparingStmt.sqlToRelConverter.getRexBuilder()
                .constantNull();
        }

        FarragoObjectCache.Entry cacheEntry =
            farragoPreparingStmt.getStmtValidator().getCodeCache().pin(
                cwmExp.refMofId(),
                this,
                false);
        RexNode parsedExp = (RexNode) cacheEntry.getValue();
        farragoPreparingStmt.getStmtValidator().getCodeCache().unpin(
            cacheEntry);

        // The expression in the cache may have been created with a different
        // type factory. Create a copy in the current type factory.
        return farragoPreparingStmt.sqlToRelConverter.getRexBuilder().copy(
            parsedExp);
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
            (CwmExpression) farragoPreparingStmt.getRepos().getMdrRepos()
            .getByMofId(mofId);
        String defaultString = cwmExp != null ? cwmExp.getBody() : "NULL";
        SqlParser sqlParser = new SqlParser(defaultString);
        SqlNode sqlNode;
        try {
            sqlNode = sqlParser.parseExpression();
        } catch (SqlParseException ex) {
            // parsing of expressions already stored in the catalog should
            // always succeed
            throw Util.newInternal(ex);
        }
        RexNode exp =
            farragoPreparingStmt.sqlToRelConverter.convertExpression(sqlNode);

        // TODO:  better memory usage estimate
        entry.initialize(
            exp,
            3 * FarragoUtil.getStringMemoryUsage(defaultString),
            true);
    }

    // implement CachedObjectFactory
    public boolean isStale(Object value)
    {
        return false;
    }
}

// End ReposDefaultValueFactory.java
