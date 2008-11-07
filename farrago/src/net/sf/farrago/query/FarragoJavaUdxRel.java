/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
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

import java.util.List;

import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.FarragoSessionRuntimeContext;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.oj.stmt.OJPreparingStmt;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.runtime.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


/**
 * FarragoJavaUdxRel is the implementation for a {@link TableFunctionRel} which
 * invokes a Java UDX (user-defined transformation).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJavaUdxRel
    extends TableFunctionRelBase
    implements JavaRel
{
    //~ Instance fields --------------------------------------------------------

    private final String serverMofId;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>FarragoJavaUdxRel</code>.
     *
     * @param cluster {@link RelOptCluster}  this relational expression belongs
     * to
     * @param rexCall function invocation expression
     * @param rowType row type produced by function
     * @param serverMofId MOFID of data server to associate with this UDX
     * invocation, or null for none
     * @param inputs 0 or more relational inputs
     */
    public FarragoJavaUdxRel(
        RelOptCluster cluster,
        RexNode rexCall,
        RelDataType rowType,
        String serverMofId,
        RelNode [] inputs)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.ITERATOR),
            rexCall,
            rowType,
            inputs);
        this.serverMofId = serverMofId;
    }

    /**
     * Creates a <code>FarragoJavaUdxRel</code> with no relational inputs.
     *
     * @param cluster {@link RelOptCluster}  this relational expression belongs
     * to
     * @param rexCall function invocation expression
     * @param rowType row type produced by function
     * @param serverMofId MOFID of data server to associate with this UDX
     * invocation, or null for none
     */
    public FarragoJavaUdxRel(
        RelOptCluster cluster,
        RexNode rexCall,
        RelDataType rowType,
        String serverMofId)
    {
        this(cluster, rexCall, rowType, serverMofId, RelNode.emptyArray);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Constructs a new instance of {@link FarragoJavaUdxRel} via a lookup from
     * the catalog. This is intended for use by optimizer rules which need to
     * insert system-defined UDX invocations into a plan.
     *
     * @param preparingStmt statement being prepared
     * @param rowType type descriptor for UDX output row
     * @param udxSpecificName specific name with which the UDX was created
     * (either via the SPECIFIC keyword or the invocation name if SPECIFIC was
     * not specified); this can be a qualified name, possibly with quoted
     * identifiers, e.g. x.y.z or x."y".z
     * @param serverMofId if not null, the invoked UDX can access a SQL/MED data
     * server with the given MOFID at runtime via {@link
     * FarragoUdrRuntime#getDataServerRuntimeSupport}
     * @param args arguments to UDX invocation
     * @param relInputs relational inputs
     */
    public static RelNode newUdxRel(
        FarragoPreparingStmt preparingStmt,
        RelDataType rowType,
        String udxSpecificName,
        String serverMofId,
        RexNode [] args,
        RelNode [] relInputs)
    {
        // Parse the specific name of the UDX.
        SqlIdentifier udxId;
        try {
            SqlParser parser = new SqlParser(udxSpecificName);
            SqlNode parsedId = parser.parseExpression();
            udxId = (SqlIdentifier) parsedId;
        } catch (Exception ex) {
            throw FarragoResource.instance().MedInvalidUdxId.ex(
                udxSpecificName,
                ex);
        }

        // Look up the UDX in the catalog.
        List<SqlOperator> list =
            preparingStmt.getSqlOperatorTable().lookupOperatorOverloads(
                udxId,
                SqlFunctionCategory.UserDefinedSpecificFunction,
                SqlSyntax.Function);
        FarragoUserDefinedRoutine udx = null;
        if (list.size() == 1) {
            SqlOperator obj = list.get(0);
            if (obj instanceof FarragoUserDefinedRoutine) {
                udx = (FarragoUserDefinedRoutine) obj;
                if (!FarragoCatalogUtil.isTableFunction(udx.getFemRoutine())) {
                    // Not a UDX.
                    udx = null;
                }
            }
        }
        if (udx == null) {
            throw FarragoResource.instance().MedUnknownUdx.ex(
                udxId.toString());
        }

        // UDX wants all types nullable, so construct a corresponding
        // type descriptor for the result of the call.
        RelOptCluster cluster = preparingStmt.getRelOptCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelDataType resultType =
            typeFactory.createTypeWithNullability(
                rowType,
                true);

        // Create a relational algebra expression for invoking the UDX.
        RexNode rexCall = rexBuilder.makeCall(udx, args);
        RelNode udxRel =
            new FarragoJavaUdxRel(
                cluster,
                rexCall,
                resultType,
                serverMofId,
                relInputs);

        // Optimizer wants us to preserve original types,
        // so cast back for the final result.
        return RelOptUtil.createCastRel(
            udxRel,
            rowType,
            true);
    }

    // implement RelNode
    public FarragoJavaUdxRel clone()
    {
        FarragoJavaUdxRel clone =
            new FarragoJavaUdxRel(
                getCluster(),
                getCall(),
                getRowType(),
                serverMofId,
                RelOptUtil.clone(inputs));
        clone.inheritTraitsFrom(this);
        clone.setColumnMappings(getColumnMappings());
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO jvs 8-Jan-2006:  get estimate from user or history
        return planner.makeTinyCost();
    }

    // override TableFunctionRelBase
    public void explain(RelOptPlanWriter pw)
    {
        if (serverMofId == null) {
            super.explain(pw);
            return;
        }

        // NOTE jvs 7-Mar-2006:  including the serverMofId means
        // we can't use EXPLAIN PLAN in diff-based testing because
        // the MOFID isn't deterministic.

        pw.explain(
            this,
            new String[] { "invocation", "serverMofId" },
            new Object[] { serverMofId });
    }

    // implement JavaRel
    public ParseTree implement(JavaRelImplementor implementor)
    {
        final RelDataType outputRowType = getRowType();
        OJClass outputRowClass =
            OJUtil.typeToOJClass(
                outputRowType,
                implementor.getTypeFactory());
        StatementList executeMethodBody = new StatementList();
        MemberDeclarationList memberList = new MemberDeclarationList();

        // Hack to workaround the fact that janino 2.5.15 cannot see the
        // "connection" variable from an inner class nested two deep:
        //   final FarragoRuntimeContext connection =
        //       (FarragoRuntimeContext) runtimeContext;
        if (true)
        memberList.add(
            new FieldDeclaration(
                new ModifierList(ModifierList.FINAL),
                TypeName.forOJClass(
                    OJClass.forClass(FarragoRuntimeContext.class)),
                OJPreparingStmt.connectionVariable,
                new CastExpression(
                    TypeName.forOJClass(
                        OJClass.forClass(FarragoRuntimeContext.class)),
                    new Variable("runtimeContext"))));

        // Translate relational inputs to ResultSet expressions.
        final Expression [] childExprs = new Expression[inputs.length];
        for (int i = 0; i < inputs.length; ++i) {
            childExprs[i] =
                implementor.visitJavaChild(this, i, (JavaRel) inputs[i]);

            Variable varChild = implementor.newVariable();
            memberList.add(
                new FieldDeclaration(
                    new ModifierList(ModifierList.FINAL),
                    TypeName.forOJClass(OJClass.forClass(TupleIter.class)),
                    varChild.toString(),
                    childExprs[i]));
            childExprs[i] = varChild;
            if (false) // enable after we integrate latest from //open/dev
            executeMethodBody.add(
                new ExpressionStatement(
                    new MethodCall(
                        "addRestartableInput",
                        new ExpressionList(
                            varChild))));

            OJClass rowClass =
                OJUtil.typeToOJClass(
                    inputs[i].getRowType(),
                    getCluster().getTypeFactory());

            Expression typeLookupCall =
                generateTypeLookupCall(
                    implementor,
                    inputs[i]);

            ExpressionList resultSetParams = new ExpressionList();
            resultSetParams.add(childExprs[i]);
            resultSetParams.add(new ClassLiteral(rowClass));
            resultSetParams.add(typeLookupCall);
            resultSetParams.add(Literal.constantNull());

            childExprs[i] =
                new AllocationExpression(
                    OJUtil.typeNameForClass(FarragoTupleIterResultSet.class),
                    resultSetParams);

            Variable varChild2 = implementor.newVariable();
            memberList.add(
                new FieldDeclaration(
                    new ModifierList(ModifierList.FINAL),
                    OJUtil.typeNameForClass(FarragoTupleIterResultSet.class),
                    varChild2.toString(),
                    childExprs[i]));
            childExprs[i] = varChild2;
        }

        // Rebind RexInputRefs accordingly.
        final JavaRexBuilder rexBuilder =
            (JavaRexBuilder) implementor.getRexBuilder();
        RexShuttle shuttle =
            new RexShuttle() {
                public RexNode visitInputRef(RexInputRef inputRef)
                {
                    return rexBuilder.makeJava(
                        getCluster().getEnv(),
                        childExprs[inputRef.getIndex()]);
                }
            };

        RexNode rewrittenCall = getCall().accept(shuttle);

        // Set up server MOFID context while generating method call
        // so that it will be available to the UDX at runtime in case
        // it needs to call back to the foreign data server.
        FarragoRelImplementor farragoImplementor =
            (FarragoRelImplementor) implementor;
        farragoImplementor.setServerMofId(serverMofId);
        implementor.translateViaStatements(
            this,
            rewrittenCall,
            executeMethodBody,
            memberList);
        farragoImplementor.setServerMofId(null);

        // Call QueueIterator's done method to indicate end-of-stream:
        //     done(null);
        executeMethodBody.add(
            new ExpressionStatement(
                new MethodCall(
                    (Expression) null,
                    "done",
                    new ExpressionList(
                        Literal.constantNull()))));

        MemberDeclaration executeMethodDecl =
            new MethodDeclaration(
                new ModifierList(ModifierList.PROTECTED),
                TypeName.forOJClass(OJSystem.VOID),
                "executeUdx",
                new ParameterList(),
                null,
                executeMethodBody);
        memberList.add(executeMethodDecl);

        Expression typeLookupCall =
            generateTypeLookupCall(
                implementor,
                this);

        // both an Iterator and a TupleIter
        Expression iterExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(FarragoJavaUdxIterator.class),
                new ExpressionList(
                    implementor.getConnectionVariable(),
                    new ClassLiteral(TypeName.forOJClass(outputRowClass)),
                    typeLookupCall),
                memberList);

        return iterExp;
    }

    /**
     * Stores the row type for a relational expression in the PreparingStmt, and
     * generates a call which will retrieve it from the executable context at
     * runtime. The literal string key used is based on the relational
     * expression id.
     */
    private Expression generateTypeLookupCall(
        JavaRelImplementor implementor,
        RelNode relNode)
    {
        String resultSetName = "ResultSet:" + relNode.getId();
        FarragoPreparingStmt preparingStmt =
            ((FarragoRelImplementor) implementor).getPreparingStmt();
        preparingStmt.mapResultSetType(
            resultSetName,
            relNode.getRowType());

        MethodCall typeLookupCall =
            new MethodCall(
                implementor.getConnectionVariable(),
                "getRowTypeForResultSet",
                new ExpressionList(
                    Literal.makeLiteral(resultSetName)));

        return typeLookupCall;
    }
}

// End FarragoJavaUdxRel.java
