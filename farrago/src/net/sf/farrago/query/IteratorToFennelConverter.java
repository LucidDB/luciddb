/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

import java.lang.reflect.*;
import java.nio.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;
import net.sf.farrago.util.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * IteratorToFennelConverter is a Converter from the
 * {@link CallingConvention#ITERATOR iterator calling convention} to the
 * {@link FennelPullRel#FENNEL_PULL_CONVENTION fennel calling convention}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class IteratorToFennelConverter extends ConverterRel
    implements FennelPullRel
{
    //~ Instance fields -------------------------------------------------------

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new IteratorToFennelConverter object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child input rel producing rows to be converted to Fennel
     */
    public IteratorToFennelConverter(RelOptCluster cluster,
        RelNode child)
    {
        super(cluster, child);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public CallingConvention getConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    // implement RelNode
    public Object clone()
    {
        return new IteratorToFennelConverter(cluster, child);
    }

    public static Expression generateTupleWriter(
        FarragoPreparingStmt stmt,
        JavaRelImplementor implementor,
        RelDataType rowType)
    {
        FarragoTypeFactory factory = stmt.getFarragoTypeFactory();

        OJClass ojClass = OJUtil.typeToOJClass(rowType, factory);

        FemTupleDescriptor tupleDesc =
            FennelRelUtil.createTupleDescriptorFromRowType(
                stmt.getRepos(),
                factory,
                rowType);
        FemTupleAccessor tupleAccessor =
            FennelRelUtil.getAccessorForTupleDescriptor(
                stmt.getRepos(),
                stmt.getFennelDbHandle(),
                tupleDesc);

        // we're going to build up a long method body
        StatementList methodBody = new StatementList();

        // define tuple variable, casting to correct row type
        Variable varTuple = implementor.newVariable();
        methodBody.add(
            new VariableDeclaration(
                TypeName.forOJClass(ojClass),
                varTuple.toString(),
                new CastExpression(
                    TypeName.forOJClass(ojClass),
                    new FieldAccess("object"))));

        if (tupleAccessor.getBitFieldOffset() != -1) {
            // generate code to marshal all bits fields at once
            methodBody.add(
                new ExpressionStatement(
                    new MethodCall(
                        new FieldAccess(varTuple.toString()),
                        "marshalBitFields",
                        new ExpressionList(
                            new FieldAccess("sliceBuffer"),
                            Literal.makeLiteral(
                                tupleAccessor.getBitFieldOffset())))));
        }

        // for each field in synthetic object, generate the type-appropriate
        // code with help from the tuple accessor
        RelDataTypeField [] fields = rowType.getFields();
        assert (fields.length == tupleAccessor.getAttrAccessor().size());
        Iterator attrIter = tupleAccessor.getAttrAccessor().iterator();
        boolean variableWidth = false;
        for (int i = 0; i < fields.length; ++i) {
            FemTupleAttrAccessor attrAccessor =
                (FemTupleAttrAccessor) attrIter.next();
            if (attrAccessor.getBitValueIndex() != -1) {
                // bit fields are already handled
                continue;
            }
            RelDataTypeField field = fields[i];
            RelDataType type = field.getType();
            Expression fieldExp =
                new FieldAccess(varTuple,
                    Util.toJavaId(
                        field.getName(),
                        i));
            Class primitiveClass = factory.getClassForPrimitive(type);
            if (primitiveClass != null) {
                Method method =
                    ReflectUtil.getByteBufferWriteMethod(primitiveClass);
                String byteBufferAccessorName = method.getName();

                // this field is marshalled to a fixed offset relative
                // to the sliceBuffer start
                fieldExp = factory.getValueAccessExpression(type, fieldExp);

                // REVIEW:  skip write if field is null?
                methodBody.add(
                    new ExpressionStatement(
                        new MethodCall(
                            new FieldAccess("sliceBuffer"),
                            byteBufferAccessorName,
                            new ExpressionList(
                                Literal.makeLiteral(
                                    attrAccessor.getFixedOffset()),
                                fieldExp))));
            } else if (SqlTypeUtil.isBoundedVariableWidth(type)) {
                variableWidth = true;
                if (attrAccessor.getFixedOffset() != -1) {
                    // first variable-width field:  position to the start of the
                    // variable width data
                    methodBody.add(
                        new ExpressionStatement(
                            new MethodCall(
                                new FieldAccess("sliceBuffer"),
                                "position",
                                new ExpressionList(
                                    Literal.makeLiteral(
                                        attrAccessor.getFixedOffset())))));
                } else {
                    // use position set by previous variable-width field
                }

                // write data
                methodBody.add(
                    new ExpressionStatement(
                        new MethodCall(
                            fieldExp,
                            "writeToBuffer",
                            new ExpressionList(new FieldAccess("sliceBuffer")))));

                // position after data has been written is the end
                // offset for this field
                Expression currPosExp =
                    new CastExpression(OJSystem.SHORT,
                        new MethodCall(new FieldAccess("sliceBuffer"),
                            "position",
                            new ExpressionList()));

                // write the end indirect offset
                methodBody.add(
                    new ExpressionStatement(
                        new MethodCall(
                            new FieldAccess("sliceBuffer"),
                            "putShort",
                            new ExpressionList(
                                Literal.makeLiteral(
                                    attrAccessor.getEndIndirectOffset()),
                                currPosExp))));
            } else {
                // fixed-width CHARACTER or BINARY
                // TODO:  ensure that data is already blank/zero padded
                methodBody.add(
                    new ExpressionStatement(
                        new MethodCall(
                            fieldExp,
                            "writeToBufferAbsolute",
                            new ExpressionList(
                                new FieldAccess("sliceBuffer"),
                                Literal.makeLiteral(
                                    attrAccessor.getFixedOffset())))));
            }
        }

        if (!variableWidth) {
            // no variable width fields, so set position to fixed end
            methodBody.add(
                new ExpressionStatement(
                    new MethodCall(
                        new FieldAccess("sliceBuffer"),
                        "position",
                        new ExpressionList(
                            Literal.makeLiteral(
                                tupleAccessor.getMinByteLength())))));
        }

        // method parameter list matches FennelTupleWriter.marshalTupleOrThrow
        ParameterList paramList = new ParameterList();
        paramList.add(
            new Parameter(
                new ModifierList(0),
                OJUtil.typeNameForClass(ByteBuffer.class),
                "sliceBuffer"));
        paramList.add(
            new Parameter(
                new ModifierList(0),
                TypeName.forOJClass(OJSystem.OBJECT),
                "object"));

        // put it all together
        MemberDeclaration methodDecl =
            new MethodDeclaration(new ModifierList(ModifierList.PROTECTED),
                TypeName.forOJClass(OJSystem.VOID), "marshalTupleOrThrow",
                paramList, null, methodBody);

        // generate code to allocate instance of anonymous class defined above
        return new AllocationExpression(
            OJUtil.typeNameForClass(FennelTupleWriter.class),
            new ExpressionList(),
            new MemberDeclarationList(methodDecl));
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        if (getInputConvention().getOrdinal() != CallingConvention.ITERATOR_ORDINAL) {
            throw cannotImplement();
        }

        RelDataType rowType = child.getRowType();

        // Cheeky! We happen to know it's a FarragoRelImplementor (for now).
        JavaRelImplementor javaRelImplementor =
            (JavaRelImplementor) implementor;

        // Generate code for children, producing the iterator expression
        // whose results are to be converted.
        Expression childExp =
            javaRelImplementor.visitJavaChild(this, 0, (JavaRel) child);

        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(this);
        Expression newTupleWriterExp =
            generateTupleWriter(stmt, javaRelImplementor, rowType);

        // and pass this to FarragoRuntimeContext.newJavaTupleStream to produce
        // a JavaTupleStream, which will invoke our generated FennelTupleWriter
        // to marshal
        ExpressionList exprList = new ExpressionList();
        exprList.add(Literal.makeLiteral(getId()));
        exprList.add(newTupleWriterExp);
        exprList.add(childExp);
        return new MethodCall(
            stmt.getConnectionVariable(),
            "newJavaTupleStream",
            exprList);
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);

        FemJavaTupleStreamDef streamDef = repos.newFemJavaTupleStreamDef();
        streamDef.setStreamId(getId());

        return streamDef;
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        // TODO:  propagate this information through Java XO's?
        return RelFieldCollation.emptyCollationArray;
    }

    /**
     * Registers this relational expression and rule(s) with the planner, as
     * per {@link AbstractRelNode#register}.
     *
     * @param planner Planner
     */
    public static void register(RelOptPlanner planner)
    {
        planner.addRule(
            new ConverterRule(RelNode.class, CallingConvention.ITERATOR,
                FennelPullRel.FENNEL_PULL_CONVENTION,
                "IteratorToFennelPullRule") {
                public RelNode convert(RelNode rel)
                {
                    return new IteratorToFennelConverter(rel.getCluster(),
                        rel);
                }
            });
    }
}


// End IteratorToFennelConverter.java
