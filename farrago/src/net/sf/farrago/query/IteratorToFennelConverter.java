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
import java.util.logging.Logger;
import java.util.logging.Level;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.type.*;
import net.sf.farrago.trace.FarragoTrace;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.stmt.OJPreparingStmt;
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
 * {@link FennelRel#FENNEL_EXEC_CONVENTION fennel calling convention}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class IteratorToFennelConverter extends ConverterRel
    implements FennelRel
{
    // REVIEW: SWZ: 3/8/2006: These really belong elsewhere.  Perhaps
    // FarragoRelImplementor?  Note that "connection" in particular 
    // appears in FennelToIteratorConverter.
    public static final String CONNECTION_VAR_NAME = "connection";
    public static final String INPUT_BINDINGS_VAR_NAME = "inputBindings";
    public static final String STREAM_NAME_VAR_NAME = 
        "farragoTransformStreamName";
    
    public static final IteratorToFennelPullRule Rule =
        new IteratorToFennelPullRule();
    protected static final Logger tracer = FarragoTrace.getPlanDumpTracer();

    //~ Instance fields -------------------------------------------------------

    private int javaImplInvocationCount;
    private int fennelImplInvocationCount;
    
    private Map<Integer, String> farragoTransformClassNameMap;

    private Map<Integer, List<FemExecutionStreamDef>> childStreamDefsMap;
    
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new IteratorToFennelConverter object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child input rel producing rows to be converted to Fennel
     */
    public IteratorToFennelConverter(
        RelOptCluster cluster,
        RelNode child)
    {
        super(
            cluster, CallingConventionTraitDef.instance,
            new RelTraitSet(FENNEL_EXEC_CONVENTION), child);
        
        farragoTransformClassNameMap = new HashMap<Integer, String>();
        javaImplInvocationCount = 0;
        fennelImplInvocationCount = 0;
        childStreamDefsMap = 
            new HashMap<Integer, List<FemExecutionStreamDef>>();
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public Object clone()
    {
        IteratorToFennelConverter clone =
            new IteratorToFennelConverter(getCluster(), getChild());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public static Expression generateTupleWriter(
        FarragoPreparingStmt stmt,
        JavaRelImplementor implementor,
        RelDataType rowType)
    {
        FarragoTypeFactory factory =
            (FarragoTypeFactory) implementor.getTypeFactory();
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
    
    private static ClassDeclaration generateTransformer(
        FarragoPreparingStmt stmt,
        String className,
        FarragoRelImplementor implementor,
        Expression tupleWriterExpression,
        Expression childExp)
    {

        MemberDeclarationList memberList = new MemberDeclarationList();
        
        // public class TransformerX extends FarragoTransformImpl
        //    implements FarragoTransform 
        // {
        ClassDeclaration transformerDecl = 
            new ClassDeclaration(
                new ModifierList(ModifierList.PUBLIC),
                className,
                new TypeName[] {
                    OJUtil.typeNameForClass(FarragoTransformImpl.class)
                },
                new TypeName[] { 
                    OJUtil.typeNameForClass(FarragoTransform.class)
                },
                memberList);
        
        //     public void init(
        //         FarragoRuntimeContext connection,
        //         FarragoTransformInputBinding[] bindings)
        //     {
        ParameterList initParams = new ParameterList();
        initParams.add(
            new Parameter(
                new ModifierList(ModifierList.FINAL),
                OJUtil.typeNameForClass(FarragoRuntimeContext.class),
                OJPreparingStmt.connectionVariable));
        initParams.add(
            new Parameter(
                new ModifierList(ModifierList.EMPTY),
                OJUtil.typeNameForClass(String.class),
                STREAM_NAME_VAR_NAME));
        initParams.add(
            new Parameter(
                new ModifierList(ModifierList.EMPTY),
                OJUtil.typeNameForClass(FarragoTransform.InputBinding[].class),
                INPUT_BINDINGS_VAR_NAME));
        
        StatementList initBody = new StatementList();
        
        //         super.init(
        //             new FennelTupleWriter() { ... },
        //             new TupleIter(...) { ... });
        // (The TupleIter will be based on one or more calls to
        //  connection.newFennelTransformTupleIter, to which inputBindings
        //  is passed.)
        ExpressionList superInitParamsList = new ExpressionList();
        initBody.add(
            new ExpressionStatement(
                new MethodCall(
                    SelfAccess.makeSuper(),
                    "init",
                    superInitParamsList)));
        superInitParamsList.add(tupleWriterExpression);
        superInitParamsList.add(childExp);
        
        MethodDeclaration initMethod = 
            new MethodDeclaration(
                new ModifierList(ModifierList.PUBLIC),
                TypeName.forOJClass(OJSystem.VOID),
                "init",
                initParams,
                null,
                initBody);
        //     }  // End of init
        // } // End of class
        
        memberList.add(initMethod);
        
        return transformerDecl;
    }

    protected void initJavaInvocation()
    {
        javaImplInvocationCount++;        

        // init child stream defs for this invocation
        childStreamDefsMap.put(
            javaImplInvocationCount, 
            new ArrayList<FemExecutionStreamDef>());
    }
    
    protected void initFennelInvocation()
    {
        fennelImplInvocationCount++;
    }
    
    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        if (getInputConvention().getOrdinal() != CallingConvention.ITERATOR_ORDINAL) {
            throw cannotImplement();
        }

        initJavaInvocation();
        
        // Cheeky! We happen to know it's a FarragoRelImplementor (for now).
        FarragoRelImplementor farragoRelImplementor =
            (FarragoRelImplementor) implementor;

        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(this);

        String baseClassName =
            "Transformer" + farragoRelImplementor.allocateTransform();
    
        String transformClassName =
            stmt.getEnvironment().getPackage() + "." + baseClassName;
    
        setFarragoTransformClassName(transformClassName);

        RelDataType rowType = getChild().getRowType();

        // Generate code for children, producing the iterator expression
        // whose results are to be converted.
        Expression childExp =
            farragoRelImplementor.visitJavaChild(this, 0, (JavaRel) getChild());

        Expression newTupleWriterExp =
            generateTupleWriter(stmt, farragoRelImplementor, rowType);

        ClassDeclaration transformDecl = generateTransformer(
            stmt, 
            baseClassName,
            farragoRelImplementor,
            newTupleWriterExp,
            childExp);
            
        farragoRelImplementor.addTransform(transformDecl);
            
        ParseTree parseTree = Literal.constantNull();

        if (tracer.isLoggable(Level.FINE)) {
            tracer.log(
                Level.FINE,
                "Parse tree for IteratorToFennelConverter",
                new Object [] { parseTree });
        }

        return parseTree;
    }

    /**
     * Registers the FemExecutionStreamDef(s) that form the inputs to this
     * converter's FemExecutionStreamDef.
     * 
     * @param childStreamDef child stream def to register
     */
    void registerChildStreamDef(FemExecutionStreamDef childStreamDef)
    {
        assert(childStreamDefsMap.containsKey(javaImplInvocationCount));
        
        List<FemExecutionStreamDef> childStreamDefs =
            childStreamDefsMap.get(javaImplInvocationCount);
        childStreamDefs.add(childStreamDef);
    }
    
    protected void setFarragoTransformClassName(String className)
    {
        assert(
            !farragoTransformClassNameMap.containsKey(
                javaImplInvocationCount));
        
        farragoTransformClassNameMap.put(javaImplInvocationCount, className);
    }
    
    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        initFennelInvocation();
        
        assert(
            farragoTransformClassNameMap.containsKey(
                fennelImplInvocationCount));
        assert(childStreamDefsMap.containsKey(fennelImplInvocationCount));
            
        // A single instance of this class may appear in multiple
        // locations throughout a plan.  The methods implementFennelChild
        // and toStreamDef are called once each for each location.  The
        // following assumes that order in which toStreamDef is called
        // for each location is the same as that for implementFennelChild.
        String farragoTransformClassName = 
            farragoTransformClassNameMap.get(fennelImplInvocationCount);

        List<FemExecutionStreamDef> childStreamDefs =
            childStreamDefsMap.get(fennelImplInvocationCount);
            
        FemJavaTransformStreamDef streamDef =
            implementor.getRepos().newFemJavaTransformStreamDef();

        for(FemExecutionStreamDef childStreamDef: childStreamDefs) {
            implementor.addDataFlowFromProducerToConsumer(
                childStreamDef,
                streamDef);
        }
        childStreamDefs.clear();
            
        streamDef.setStreamId(getId());
        streamDef.setJavaClassName(farragoTransformClassName);
            
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
        planner.addRule(Rule);
    }

    /**
     * Rule which converts a {@link RelNode} of
     * {@link FennelRel#FENNEL_EXEC_CONVENTION Fennel calling convention}
     * to {@link CallingConvention.ITERATOR iterator calling convention}
     * by adding a {@link IteratorToFennelConverter}.
     */
    private static class IteratorToFennelPullRule extends ConverterRule
    {
        private IteratorToFennelPullRule()
        {
            super(
                RelNode.class,
                CallingConvention.ITERATOR,
                FennelRel.FENNEL_EXEC_CONVENTION,
                "IteratorToFennelPullRule");
        }

        public RelNode convert(RelNode rel)
        {
            return new IteratorToFennelConverter(rel.getCluster(),
                rel);
        }

        public boolean isGuaranteed()
        {
            return true;
        }
    }
}


// End IteratorToFennelConverter.java
