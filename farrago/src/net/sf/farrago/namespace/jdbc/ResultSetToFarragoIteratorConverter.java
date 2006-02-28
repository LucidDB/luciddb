/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

import net.sf.farrago.query.*;
import net.sf.farrago.type.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.rel.ResultSetRel;
import org.eigenbase.oj.util.*;
import org.eigenbase.oj.rex.RexToOJTranslator;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.runtime.*;
import org.eigenbase.util.*;
import org.eigenbase.sql.type.*;


/**
 * ResultSetToFarragoIteratorConverter is a ConverterRel from the RESULT_SET
 * CallingConvention to the ITERATOR CallingConvention which ensures that
 * the objects returned by the iterator are understood by the rest
 * of Farrago.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class ResultSetToFarragoIteratorConverter extends ConverterRel
    implements JavaRel
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new ResultSetToFarragoIteratorConverter object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child input rel producing rows in ResultSet
     * representation
     */
    public ResultSetToFarragoIteratorConverter(
        RelOptCluster cluster,
        RelNode child)
    {
        super(
            cluster, CallingConventionTraitDef.instance,
            new RelTraitSet(CallingConvention.ITERATOR), child);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public Object clone()
    {
        ResultSetToFarragoIteratorConverter clone =
            new ResultSetToFarragoIteratorConverter(getCluster(), getChild());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        FarragoRelImplementor farragoImplementor =
            (FarragoRelImplementor) implementor;

        Expression childObj =
            implementor.visitJavaChild(this, 0, (ResultSetRel) getChild());

        StatementList methodBody = new StatementList();

        // variable for synthetic object instance
        Variable varTuple = implementor.newVariable();

        // variable for source ResultSet
        Variable varResultSet = new Variable("resultSet");

        // This is a little silly.  Have to put in a dummy cast
        // so that type inference will stop early (otherwise it
        // fails to find variable reference).
        Expression castResultSet =
            new CastExpression(
                TypeName.forOJClass(OJUtil.clazzResultSet),
                varResultSet);

        RelDataType rowType = getRowType();
        FarragoTypeFactory factory =
            farragoImplementor.getPreparingStmt().getFarragoTypeFactory();
        OJClass rowClass = OJUtil.typeToOJClass(rowType,factory);

        JavaRexBuilder javaRexBuilder =
            (JavaRexBuilder) getCluster().getRexBuilder();

        MemberDeclarationList memberList = new MemberDeclarationList();

        RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; ++i) {
            RelDataTypeField field = fields[i];
            RelDataType type = field.getType();
            ExpressionList colPosExpList =
                new ExpressionList(Literal.makeLiteral(i + 1));
            Expression rhsExp;
            if ((SqlTypeUtil.isJavaPrimitive(type)) && !type.isNullable()) {
                // TODO:  make this official:  java.sql and java.nio
                // use the same accessor names, happily,
                // (except for boolean, sadly)
                String methodName =
                    ReflectUtil.getByteBufferReadMethod(
                        factory.getClassForPrimitive(type)).getName();
                if (type.getSqlTypeName() == SqlTypeName.Boolean) {
                    methodName = "getBoolean";
                }
                rhsExp =
                    new MethodCall(castResultSet, methodName, colPosExpList);
            } else if (SqlTypeUtil.inCharFamily(type)) {
                rhsExp =
                    new MethodCall(castResultSet, "getString", colPosExpList);
            } else {
                rhsExp =
                    new MethodCall(castResultSet, "getObject", colPosExpList);
            }
            RexNode rhs = javaRexBuilder.makeJava(
                getCluster().getEnv(), rhsExp);
            rhs = javaRexBuilder.makeAbstractCast(
                    field.getType(),
                    rhs);
            final RexToOJTranslator translator =
                farragoImplementor.newStmtTranslator(
                    this,
                    methodBody,
                    memberList);
            translator.translateAssignment(
                field,
                new FieldAccess(
                    varTuple,
                    Util.toJavaId(
                        field.getName(),
                        i)),
                rhs);
        }

        methodBody.add(new ReturnStatement(varTuple));

        // allocate synthetic object as class data member
        memberList.add(
            new FieldDeclaration(
                new ModifierList(ModifierList.PRIVATE),
                TypeName.forOJClass(rowClass),
                varTuple.toString(),
                new AllocationExpression(
                    rowClass,
                    new ExpressionList())));

        // add method as implementation for super.makeRow
        memberList.add(
            new MethodDeclaration(
                new ModifierList(ModifierList.PUBLIC),
                TypeName.forOJClass(OJUtil.clazzObject),
                "makeRow",
                new ParameterList(),
                new TypeName [] {
                    TypeName.forOJClass(OJUtil.clazzSQLException)
                },
                methodBody));

        return new AllocationExpression(
            TypeName.forOJClass(OJClass.forClass(getResultSetIteratorClass())),
            new ExpressionList(
                new CastExpression(
                    TypeName.forOJClass(OJUtil.clazzResultSet),
                    childObj)),
            memberList);
    }
    
    private Class getResultSetIteratorClass()
    {
        if (CallingConvention.ENABLE_NEW_ITER) {
            return ResultSetTupleIter.class;
        } else {
            return ResultSetIterator.class;
        }
    }
}


// End ResultSetToFarragoIteratorConverter.java
