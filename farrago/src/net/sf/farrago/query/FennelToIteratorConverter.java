/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2005 John V. Sichi.
// Copyright (C) 2003-2005 Disruptive Tech
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

import java.io.*;
import java.lang.reflect.*;
import java.nio.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.*;
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
import org.eigenbase.util.*;
import org.eigenbase.sql.type.*;


/**
 * FennelToIteratorConverter is a Converter from the
 * {@link FennelPullRel#FENNEL_PULL_CONVENTION fennel-pull calling convention}
 * to the {@link CallingConvention#ITERATOR iterator calling convention}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelToIteratorConverter extends ConverterRel implements JavaRel
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelToIteratorConverter object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child input rel producing rows in Fennel TupleStream
     * representation
     */
    public FennelToIteratorConverter(
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
        return new FennelToIteratorConverter(cluster, child);
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        assert (child.getConvention().equals(FennelPullRel.FENNEL_PULL_CONVENTION)) : child.getClass()
            .getName();

        // Give children a chance to generate code.  Most FennelRels don't
        // require this, but IteratorToFennelConverter does.
        Expression childrenExp =
            (Expression) implementor.visitChild(this, 0, child);

        FennelRel fennelRel = (FennelRel) child;
        FarragoRepos repos = FennelRelUtil.getRepos(fennelRel);

        final FarragoPreparingStmt stmt =
            FennelRelUtil.getPreparingStmt(fennelRel);
        FarragoTypeFactory factory = stmt.getFarragoTypeFactory();

        final RelDataType rowType = getRowType();
        OJClass rowClass = OJUtil.typeToOJClass(rowType, factory);

        FennelRelImplementor farragoRelImplementor =
            (FennelRelImplementor) implementor;
        FemExecutionStreamDef rootStream =
            farragoRelImplementor.visitFennelChild((FennelRel) child);
        String rootStreamName = rootStream.getName();

        FemTupleDescriptor tupleDesc =
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                factory,
                rowType);
        FemTupleAccessor tupleAccessor =
            FennelRelUtil.getAccessorForTupleDescriptor(
                repos,
                stmt.getFennelDbHandle(),
                tupleDesc);

        // Generate
        //   connection.newFennelIterator(
        //       new FennelTupleReader(){...},
        //       << childrens' code >>);
        // The first ... requires some explanation.  Using the information
        // returned by tupleStreamDescribe, we're going to generate code to
        // unmarshal tuples, writing values into the fields of the synthetic
        // object.  This code lives in the unmarshalTuple method of an
        // anonymous subclass of FennelTupleReader.  More details on the
        // Fennel tuple format are available in the comments on the Fennel C++
        // class TupleAccessor.  (TODO:  link).  Also see Java class
        // ReflectTupleReader, which accomplishes the desired affect
        // generically, though more slowly.
        // variable for synthetic object instance
        Variable varTuple = implementor.newVariable();

        // variable for start offset of tuple in byteBuffer
        Variable varTupleStartOffset = implementor.newVariable();

        // we're going to build up a long method body
        StatementList methodBody = new StatementList();

        // get tuple start offset from current position in byteBuffer
        methodBody.add(
            new VariableDeclaration(
                TypeName.forOJClass(OJSystem.INT),
                varTupleStartOffset.toString(),
                new MethodCall(
                    new FieldAccess("byteBuffer"),
                    "position",
                    new ExpressionList())));

        if (tupleAccessor.getBitFieldOffset() != -1) {
            // generate code to unmarshal all bits fields at once
            methodBody.add(
                new ExpressionStatement(
                    new MethodCall(
                        new FieldAccess(varTuple.toString()),
                        "unmarshalBitFields",
                        new ExpressionList(
                            new FieldAccess("sliceBuffer"),
                            Literal.makeLiteral(
                                tupleAccessor.getBitFieldOffset())))));
        }

        // TODO:  reordering
        // for each field in synthetic object, generate the type-appropriate
        // code with help from the tuple accessor
        RelDataTypeField [] fields = rowType.getFields();
        Variable varPrevEndOffset = null;
        assert (fields.length == tupleAccessor.getAttrAccessor().size());
        Iterator attrIter = tupleAccessor.getAttrAccessor().iterator();
        for (int i = 0; i < fields.length; ++i) {
            FemTupleAttrAccessor attrAccessor =
                (FemTupleAttrAccessor) attrIter.next();
            if (attrAccessor.getBitValueIndex() != -1) {
                // bit fields are already handled
                continue;
            }
            RelDataTypeField field = fields[i];
            RelDataType type = field.getType();
            Class primitiveClass = factory.getClassForPrimitive(type);
            if (primitiveClass != null) {
                Method method =
                    ReflectUtil.getByteBufferReadMethod(primitiveClass);
                String byteBufferAccessorName = method.getName();

                // this field is unmarshalled from a fixed offset relative
                // to the sliceBuffer start
                Expression lhs =
                    new FieldAccess(varTuple,
                        Util.toJavaId(
                            field.getName(),
                            i));
                lhs = factory.getValueAccessExpression(type, lhs);
                methodBody.add(
                    new ExpressionStatement(
                        new AssignmentExpression(
                            lhs,
                            AssignmentExpression.EQUALS,
                            new MethodCall(
                                new FieldAccess("sliceBuffer"),
                                byteBufferAccessorName,
                                new ExpressionList(
                                    Literal.makeLiteral(
                                        attrAccessor.getFixedOffset()))))));
            } else if (SqlTypeUtil.isBoundedVariableWidth(type)) {
                // Variable-length fields are trickier.  The first one starts
                // at a fixed offset.  To determine the end, dereference the
                // indirect offset located at a fixed offset relative to the
                // sliceBuffer start.  Note that all offsets are
                // calculated relative to the start of byteBuffer, not
                // sliceBuffer, because the data is extracted from byteArray,
                // whose positions correspond with byteBuffer, not sliceBuffer.
                Variable varEndOffset = implementor.newVariable();
                methodBody.add(
                    new VariableDeclaration(
                        TypeName.forOJClass(OJSystem.INT),
                        varEndOffset.toString(),
                        new BinaryExpression(
                            varTupleStartOffset,
                            BinaryExpression.PLUS,
                            new MethodCall(
                                new FieldAccess("sliceBuffer"),
                                "getShort",
                                new ExpressionList(
                                    Literal.makeLiteral(
                                        attrAccessor.getEndIndirectOffset()))))));
                Expression expStartOffset;
                if (varPrevEndOffset == null) {
                    expStartOffset =
                        new BinaryExpression(
                            Literal.makeLiteral(attrAccessor.getFixedOffset()),
                            BinaryExpression.PLUS,
                            varTupleStartOffset);
                } else {
                    // subsequent variable-length fields start at the end of
                    // their predecessor
                    expStartOffset = varPrevEndOffset;
                }

                // Now that we know the start and end offsets, generate code to
                // extract data from byteArray.
                methodBody.add(
                    new ExpressionStatement(
                        new MethodCall(
                            new FieldAccess(
                                varTuple,
                                Util.toJavaId(
                                    field.getName(),
                                    i)),
                            BytePointer.SET_POINTER_METHOD_NAME,
                            new ExpressionList(
                                new FieldAccess("byteArray"),
                                expStartOffset,
                                varEndOffset))));
                varPrevEndOffset = varEndOffset;
            } else {
                // fixed-width CHARACTER or BINARY
                Expression expStartOffset =
                    new BinaryExpression(varTupleStartOffset,
                        BinaryExpression.PLUS,
                        Literal.makeLiteral(attrAccessor.getFixedOffset()));
                Expression expEndOffset =
                    new BinaryExpression(varTupleStartOffset,
                        BinaryExpression.PLUS,
                        Literal.makeLiteral(attrAccessor.getFixedOffset()
                            + SqlTypeUtil.getMaxByteSize(type)));
                methodBody.add(
                    new ExpressionStatement(
                        new MethodCall(
                            new FieldAccess(
                                varTuple,
                                Util.toJavaId(
                                    field.getName(),
                                    i)),
                            BytePointer.SET_POINTER_METHOD_NAME,
                            new ExpressionList(
                                new FieldAccess("byteArray"),
                                expStartOffset,
                                expEndOffset))));
            }
        }

        // calculate the end of the tuple
        Expression expTupleEndOffset;
        if (varPrevEndOffset == null) {
            // fixed-width tuple:  end is always the same
            expTupleEndOffset =
                Literal.makeLiteral(tupleAccessor.getMinByteLength());
        } else {
            // variable-width tuple:  end is same as end of last variable-width
            // field
            expTupleEndOffset =
                new BinaryExpression(varPrevEndOffset, BinaryExpression.MINUS,
                    varTupleStartOffset);
        }

        // advance sliceBuffer to tuple end
        methodBody.add(
            new ExpressionStatement(
                new MethodCall(
                    new FieldAccess("sliceBuffer"),
                    "position",
                    new ExpressionList(expTupleEndOffset))));

        // return synthetic object
        methodBody.add(
            new ReturnStatement(new FieldAccess(varTuple.toString())));

        // method parameter list matches FennelTupleReader.unmarshalTuple
        ParameterList paramList = new ParameterList();
        paramList.add(
            new Parameter(
                new ModifierList(0),
                OJUtil.typeNameForClass(ByteBuffer.class),
                "byteBuffer"));
        paramList.add(
            new Parameter(
                new ModifierList(0),
                new TypeName("byte", 1),
                "byteArray"));
        paramList.add(
            new Parameter(
                new ModifierList(0),
                OJUtil.typeNameForClass(ByteBuffer.class),
                "sliceBuffer"));

        // put it all together
        MemberDeclaration methodDecl =
            new MethodDeclaration(new ModifierList(ModifierList.PUBLIC),
                OJUtil.typeNameForClass(Object.class),
                "unmarshalTuple", paramList,
                null, methodBody);

        // allocate synthetic object as class data member
        FieldDeclaration rowVarDecl =
            new FieldDeclaration(new ModifierList(ModifierList.PRIVATE),
                TypeName.forOJClass(rowClass),
                varTuple.toString(),
                new AllocationExpression(
                    rowClass,
                    new ExpressionList()));

        // generate code to allocate instance of anonymous class defined above
        MemberDeclarationList memberDeclList = new MemberDeclarationList();
        memberDeclList.add(rowVarDecl);
        memberDeclList.add(methodDecl);
        Expression newTupleReaderExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(FennelTupleReader.class),
                new ExpressionList(),
                memberDeclList);

        // and pass this to FarragoRuntimeContext.newFennelIterator to produce a
        // FennelIterator, which will invoke our generated FennelTupleReader to
        // unmarshal
        ExpressionList argList = new ExpressionList();
        argList.add(newTupleReaderExp);
        argList.add(Literal.makeLiteral(rootStreamName));
        argList.add(childrenExp);
        return new MethodCall(
            stmt.getConnectionVariable(),
            "newFennelIterator",
            argList);
    }

    /**
     * Registers this relational expression and rule(s) with the planner, as
     * per {@link AbstractRelNode#register}.
     * @param planner Planner
     */
    public static void register(RelOptPlanner planner)
    {
        planner.addRule(
            new ConverterRule(RelNode.class,
                FennelPullRel.FENNEL_PULL_CONVENTION,
                CallingConvention.ITERATOR, "FennelToIteratorRule") {
                public RelNode convert(RelNode rel)
                {
                    return new FennelToIteratorConverter(
                        rel.getCluster(),
                        rel);
                }
            });
    }
}


// End FennelToIteratorConverter.java
