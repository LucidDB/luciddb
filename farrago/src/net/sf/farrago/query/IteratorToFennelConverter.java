/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

package net.sf.farrago.query;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;
import net.sf.farrago.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.util.Util;

import openjava.mop.*;
import openjava.ptree.*;

import java.util.*;
import java.nio.*;
import java.lang.reflect.*;


/**
 * IteratorToFennelConverter is a Converter from the
 * {@link CallingConvention#ITERATOR iterator calling convention} to the
 * {@link FennelPullRel#FENNEL_PULL_CONVENTION fennel calling convention}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class IteratorToFennelConverter
    extends ConverterRel
    implements FennelPullRel
{
    //~ Instance fields -------------------------------------------------------

    private FarragoPreparingStmt stmt;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new IteratorToFennelConverter object.
     *
     * @param stmt statement to use for catalog access
     * @param cluster RelOptCluster for this rel
     * @param child input rel producing rows to be converted to Fennel
     * TupleStream representation
     */
    public IteratorToFennelConverter(
        FarragoPreparingStmt stmt,
        RelOptCluster cluster,
        RelNode child)
    {
        super(cluster,child);
        this.stmt = stmt;
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public CallingConvention getConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    // implement FennelRel
    public FarragoPreparingStmt getPreparingStmt()
    {
        return stmt;
    }

    // implement RelNode
    public Object clone()
    {
        return new IteratorToFennelConverter(stmt,cluster,child);
    }

    public static Expression generateTupleWriter(
        FarragoPreparingStmt stmt,
        JavaRelImplementor implementor,
        RelDataType rowType)
    {
        OJClass ojClass = OJUtil.typeToOJClass(rowType);

        FemTupleDescriptor tupleDesc =
            FennelRelUtil.createTupleDescriptorFromRowType(
                stmt.getRepos(),
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
            FarragoAtomicType type = (FarragoAtomicType) field.getType();
            Expression fieldExp = new FieldAccess(varTuple,
                Util.toJavaId(field.getName(),i));
            if (type.hasClassForPrimitive()) {
                Class primitiveClass =
                    type.getClassForPrimitive();
                Method method =
                    ReflectUtil.getByteBufferWriteMethod(primitiveClass);
                String byteBufferAccessorName = method.getName();
                // this field is marshalled to a fixed offset relative
                // to the sliceBuffer start
                if (type.requiresValueAccess()) {
                    // extra dereference for NullablePrimitives
                    fieldExp = new FieldAccess(
                        fieldExp,NullablePrimitive.VALUE_FIELD_NAME);
                }
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
            } else if (type.isBoundedVariableWidth()) {
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
                            new ExpressionList(
                                new FieldAccess("sliceBuffer")))));
                // position after data has been written is the end
                // offset for this field
                Expression currPosExp = new CastExpression(
                    OJSystem.SHORT,
                    new MethodCall(
                        new FieldAccess("sliceBuffer"),
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
                TypeName.forClass(ByteBuffer.class),
                "sliceBuffer"));
        paramList.add(
            new Parameter(
                new ModifierList(0),
                TypeName.forOJClass(OJSystem.OBJECT),
                "object"));

        // put it all together
        MemberDeclaration methodDecl =
            new MethodDeclaration(
                new ModifierList(ModifierList.PROTECTED),
                TypeName.forOJClass(OJSystem.VOID),
                "marshalTupleOrThrow",
                paramList,
                null,
                methodBody);

        // generate code to allocate instance of anonymous class defined above
        return new AllocationExpression(
            TypeName.forClass(FennelTupleWriter.class),
            new ExpressionList(),
            new MemberDeclarationList(methodDecl));
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        if (getInputConvention().getOrdinal()
            != CallingConvention.ITERATOR_ORDINAL)
        {
            throw cannotImplement();
        }

        if (!getPreparingStmt().getRepos().isFennelEnabled()) {
            return Literal.constantNull();
        }

        RelDataType rowType = child.getRowType();

        // Cheeky! We happen to know it's a FarragoRelImplementor (for now).
        JavaRelImplementor javaRelImplementor = (JavaRelImplementor) implementor;
        // Generate code for children, producing the iterator expression
        // whose results are to be converted.
        Expression childExp =
                javaRelImplementor.visitJavaChild(this, 0, (JavaRel) child);

        Expression newTupleWriterExp = generateTupleWriter(
            stmt,
            javaRelImplementor,
            rowType);

        // and pass this to FarragoRuntimeContext.newJavaTupleStream to produce
        // a JavaTupleStream, which will invoke our generated FennelTupleWriter
        // to marshal
        ExpressionList exprList = new ExpressionList();
        exprList.add(Literal.makeLiteral(getId()));
        exprList.add(newTupleWriterExp);
        exprList.add(childExp);
        return new MethodCall(
            getPreparingStmt().getConnectionVariable(),
            "newJavaTupleStream",
            exprList);
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = getPreparingStmt().getRepos();

        FemJavaTupleStreamDef streamDef =
            repos.newFemJavaTupleStreamDef();
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
     * @param farragoPreparingStmt Context for the preparation process
     */
    public static void register(FarragoPlanner planner,
            final FarragoPreparingStmt farragoPreparingStmt) {
        planner.addRule(
            new ConverterRule(
                RelNode.class,
                CallingConvention.ITERATOR,
                FennelPullRel.FENNEL_PULL_CONVENTION,
                "IteratorToFennelPullRule")
            {
                public RelNode convert(RelNode rel)
                {
                    return new IteratorToFennelConverter(
                        farragoPreparingStmt,
                        rel.getCluster(),
                        rel);
                }
            });
    }
}


// End IteratorToFennelConverter.java
