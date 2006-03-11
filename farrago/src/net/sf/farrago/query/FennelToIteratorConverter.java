/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2003-2006 Disruptive Tech
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
package net.sf.farrago.query;

import java.lang.reflect.*;
import java.nio.*;
import java.util.*;
import java.util.List;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;

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
 * {@link FennelRel#FENNEL_EXEC_CONVENTION Fennel calling convention}
 * to the {@link CallingConvention#ITERATOR iterator calling convention}.
 * 
 * <p>Every FennelToIteratorConverter exists in one of two locations.
 * Location 1 means the converter is one of N roots of a sequence of JavaRels
 * that are read by FarragoResultSetIterator or FarragoResultSetTupleIter.
 * Location 2 means the converter is one of the N roots of a sequence of 
 * JavaRels that are converted back to Fennel convention by an 
 * IteratorToFennelConverter.
 * 
 * <p>When {@link CallingConvention#ENABLE_NEW_ITER} is enabled (e.g., 
 * new-style iterators are in use), it operates in one of two modes:  
 * <ol>
 * <li>
 *   When in location 1, it generates Iterator convention code that reads from 
 *   its child FennelRel, blocking when the FennelRel has no data. (This is 
 *   identical to the behavior for old-style iterators.)
 * </li>
 * <li>
 *   When in location 2, it generates Iterator convention code that reads from
 *   its child FennelRel, but returns
 *   {@link org.eigenbase.runtime.TupleIter.NoDataReason#UNDERFLOW} when the
 *   FennelRel has no data.  In addition, the generated code is not 
 *   encapsulated in a method and returned to the caller of 
 *   {@link #implement(JavaRelImplementor)}.  Instead it's registered with
 *   the {@link FarragoRelImplementor} for later compilation.  In addition,
 *   the stream def generated for child FennelRels is stored for later use
 *   by IteratorToFennelConverter.
 * </li>
 * </ol> 
 *
 * REVIEW: SWZ 3/7/2006: It would be nice if this could be split into two
 * converters.  One for Location 1 and another for Location 2.  This would
 * simplify the code and make it easier to understand.  Not sure if the
 * planner can handle two converters from Fennel to Iterator that need to
 * be used under different circumstances, though.
 * 
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelToIteratorConverter extends ConverterRel implements JavaRel
{
    /**
     * The singleton rule which uses a {@link FennelToIteratorConverter} to
     * convert a {@link RelNode}
     * from {@link FennelRel#FENNEL_EXEC_CONVENTION} convention
     * to {@link CallingConvention#ITERATOR} convention.
     */
    public static final ConverterRule Rule = new ConverterRule(
        RelNode.class,
        FennelRel.FENNEL_EXEC_CONVENTION,
        CallingConvention.ITERATOR, "FennelToIteratorRule") {
        public RelNode convert(RelNode rel)
        {
            return new FennelToIteratorConverter(
                rel.getCluster(),
                rel);
        }
    };

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
        super(
            cluster, CallingConventionTraitDef.instance,
            new RelTraitSet(CallingConvention.ITERATOR), child);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public Object clone()
    {
        FennelToIteratorConverter clone =
            new FennelToIteratorConverter(getCluster(), getChild());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public ParseTree implement(JavaRelImplementor implementor)
    {
        assert (getChild().getConvention().equals(
                    FennelRel.FENNEL_EXEC_CONVENTION))
            : getChild().getClass().getName();

        boolean useTransformer = false;
        if (CallingConvention.ENABLE_NEW_ITER) {
            if (isTransformerInput(implementor)) {
                useTransformer = true;
            }
        }
        
        // Give children a chance to generate code.  Most FennelRels don't
        // require this, but IteratorToFennelConverter does. 
        // REVIEW: SWZ: 3/1/06: True for old-style iterators only, but doesn't
        // hurt since IteratorToFennelConverter will return a null literal
        // when this is superfluous.
        Expression childrenExp =
            (Expression) implementor.visitChild(this, 0, getChild());

        FennelRelImplementor fennelImplementor = 
            (FennelRelImplementor) implementor;
        FennelRel fennelRel = (FennelRel) getChild();
        FarragoRepos repos = fennelImplementor.getRepos();

        final FarragoPreparingStmt stmt =
            FennelRelUtil.getPreparingStmt(fennelRel);
        FarragoTypeFactory factory;
        FennelDbHandle dbHandle;
        Variable connectionVariable;
        if (stmt == null) {
            factory = (FarragoTypeFactory) getCluster().getTypeFactory();
            dbHandle = null;
            connectionVariable = new Variable("connection");
        } else {
            factory = stmt.getFarragoTypeFactory();
            dbHandle = stmt.getFennelDbHandle();
            connectionVariable = stmt.getConnectionVariable();
        }

        final RelDataType rowType = getRowType();
        OJClass rowClass = OJUtil.typeToOJClass(rowType, factory);

        // Implement the child rel as an XO.
        FemExecutionStreamDef rootStream = childToStreamDef(fennelImplementor);
        String rootStreamName = rootStream.getName();
        int rootStreamId = getId();
        
        FemTupleDescriptor tupleDesc =
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                factory,
                rowType);
        FemTupleAccessor tupleAccessor =
            FennelRelUtil.getAccessorForTupleDescriptor(
                repos,
                dbHandle,
                tupleDesc);

        // For new iter convention generate:
        //   connection.newFennelTupleIter(
        //       new FennelTupleReader(){...},
        //       << childrens' code >>);
        // else for old iter convention, generate
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

        if (!useTransformer) {
            // Pass tuple reader to FarragoRuntimeContext.newFennelIterator to 
            // produce a FennelIterator, which will invoke our generated 
            // FennelTupleReader to unmarshal
            ExpressionList argList = new ExpressionList();
            argList.add(newTupleReaderExp);
            argList.add(Literal.makeLiteral(rootStreamName));
            argList.add(Literal.makeLiteral(rootStreamId));
            argList.add(childrenExp);
            
            if (CallingConvention.ENABLE_NEW_ITER) {
                return new MethodCall(
                    connectionVariable,
                    "newFennelTupleIter",
                    argList);
            } else {
                return new MethodCall(
                    connectionVariable,
                    "newFennelIterator",
                    argList);
            }
        } else {
            // Pass tuple reader to 
            // FarragoRuntimeContext.newFennelTransformTupleIter to produce
            // a FennelTupleIter, which will invoke our generated 
            // FennelTupleReader to unmarshal
            assert(CallingConvention.ENABLE_NEW_ITER);
            
            // IteratorToFennelConverter will just return a literal null.
            // FennelDoubleRel will return a MethodCall to 
            // FarragoRuntimeContext.dummyPair().  FennelMultipleRel returns
            // a MethodCall to FarragoRuntimeContext.dummArray().
            // This assert isn't really necessary -- we're just trying to 
            // assert that the children's code generation didn't place code
            // here -- we want it in a separate class that implements
            // FarragoTransform.
            assert(
                (childrenExp instanceof Literal &&
                    ((Literal)childrenExp).getLiteralType() == Literal.NULL) || 
                (childrenExp instanceof MethodCall &&
                    ((MethodCall)childrenExp).getName().startsWith("dummy"))):
                        childrenExp.toString();
                
            // Register this stream def with our ancestral 
            // IteratorToFennelConverter.  Note that this converter instance
            // might appear in several branches of the planner's tree (e.g.,
            // it can have different ancestors at different times)
            registerChildWithAncestor(implementor, rootStream);
            
            ExpressionList argList = new ExpressionList();
            argList.add(newTupleReaderExp);
            argList.add(
                new Variable(IteratorToFennelConverter.STREAM_NAME_VAR_NAME));
            argList.add(Literal.makeLiteral(rootStreamName));
            argList.add(
                new Variable(
                    IteratorToFennelConverter.INPUT_BINDINGS_VAR_NAME));
            argList.add(childrenExp);

            return new MethodCall(
                connectionVariable,
                "newFennelTransformTupleIter",
                argList);
        }
    }

    /**
     * Converts the child relational expression (which is in Fennel
     * convention) into a {@link FemExecutionStreamDef}.
     *
     * <p>Derived classes may override this method.
     */
    protected FemExecutionStreamDef childToStreamDef(
        FennelRelImplementor implementor)
    {
        FemExecutionStreamDef rootStream =
            implementor.visitFennelChild((FennelRel) getChild());
        return rootStream;
    }

    /**
     * Determines whether this FennelToIteratorConverter is an input to
     * a FarragoTransform.  In other words, is one of the ancestors to
     * this rel an IteratorToFennelConverter.
     * 
     * @param implementor implementor in use
     * @return true if this rel is an input to a FarragoTransform, false 
     *         otherwise
     */
    protected boolean isTransformerInput(JavaRelImplementor implementor)
    {
        List ancestors = implementor.getAncestorRels(this);
        for(Object o: ancestors) {
            RelNode ancestor = (RelNode)o;
            
            if (ancestor.getConvention() == FennelRel.FENNEL_EXEC_CONVENTION) {
                assert(ancestor instanceof IteratorToFennelConverter);
                return true;
            }
        }
        
        return false;
    }
    
    private void registerChildWithAncestor(
        JavaRelImplementor implementor, FemExecutionStreamDef streamDef)
    {
        List ancestors = implementor.getAncestorRels(this);
        for(Object temp: ancestors) {
            RelNode ancestor = (RelNode)temp;
            
            if (ancestor instanceof IteratorToFennelConverter) {
                ((IteratorToFennelConverter)ancestor).registerChildStreamDef(
                    streamDef);
                return;
            }
        }
        
        assert(false): "Ancestor IteratorToFennelConverter not found";
    }
    
    /**
     * Registers this relational expression and rule(s) with the planner, as
     * per {@link AbstractRelNode#register}.
     * @param planner Planner
     */
    public static void register(RelOptPlanner planner)
    {
        planner.addRule(Rule);
    }
}


// End FennelToIteratorConverter.java
