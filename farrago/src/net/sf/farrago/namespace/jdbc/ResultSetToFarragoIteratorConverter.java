/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.namespace.jdbc;

import java.io.*;
import java.util.*;

import net.sf.farrago.query.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.rel.ResultSetRel;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.runtime.*;
import org.eigenbase.util.*;


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
        super(cluster, child);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public CallingConvention getConvention()
    {
        return CallingConvention.ITERATOR;
    }

    // implement RelNode
    public Object clone()
    {
        return new ResultSetToFarragoIteratorConverter(cluster, child);
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        FarragoRelImplementor farragoImplementor =
            (FarragoRelImplementor) implementor;

        Expression childObj =
            implementor.visitJavaChild(this, 0, (ResultSetRel) child);

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
        OJClass rowClass = OJUtil.typeToOJClass(rowType);

        JavaRexBuilder javaRexBuilder =
            (JavaRexBuilder) getCluster().rexBuilder;

        MemberDeclarationList memberList = new MemberDeclarationList();

        RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; ++i) {
            RelDataTypeField field = fields[i];
            FarragoAtomicType type = (FarragoAtomicType) field.getType();
            ExpressionList colPosExpList =
                new ExpressionList(Literal.makeLiteral(i + 1));
            Expression rhsExp;
            if ((type instanceof FarragoPrimitiveType) && !type.isNullable()) {
                FarragoPrimitiveType primType = (FarragoPrimitiveType) type;

                // TODO:  make this official:  java.sql and java.nio
                // use the same accessor names, happily
                String methodName =
                    ReflectUtil.getByteBufferReadMethod(
                        primType.getClassForPrimitive()).getName();
                rhsExp =
                    new MethodCall(castResultSet, methodName, colPosExpList);
            } else if (type.isCharType()) {
                rhsExp =
                    new MethodCall(castResultSet, "getString", colPosExpList);
            } else {
                rhsExp =
                    new MethodCall(castResultSet, "getObject", colPosExpList);
            }
            RexNode rhs = javaRexBuilder.makeJava(getCluster().env, rhsExp);
            rhs = javaRexBuilder.makeAbstractCast(
                    field.getType(),
                    rhs);
            farragoImplementor.translateAssignment(
                this,
                field.getType(),
                new FieldAccess(
                    varTuple,
                    Util.toJavaId(
                        field.getName(),
                        i)),
                rhs,
                methodBody,
                memberList);
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
            TypeName.forOJClass(OJClass.forClass(ResultSetIterator.class)),
            new ExpressionList(
                new CastExpression(
                    TypeName.forOJClass(OJUtil.clazzResultSet),
                    childObj)),
            memberList);
    }
}


// End ResultSetToFarragoIteratorConverter.java
