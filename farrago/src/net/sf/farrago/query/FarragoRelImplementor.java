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

import java.util.*;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.ojrex.*;
import net.sf.farrago.type.runtime.*;
import net.sf.farrago.util.*;
import net.sf.farrago.catalog.FarragoRepos;
import net.sf.farrago.runtime.FarragoTransform;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.rex.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * FarragoRelImplementor refines {@link JavaRelImplementor} with some Farrago
 * specifics.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRelImplementor extends JavaRelImplementor
    implements FennelRelImplementor, FarragoOJRexRelImplementor
{
    //~ Instance fields -------------------------------------------------------

    FarragoPreparingStmt preparingStmt;
    OJClass ojAssignableValue;
    OJClass ojBytePointer;
    private Set streamDefSet;
    private String serverMofId;
    private int nextParamId;

    //~ Constructors ----------------------------------------------------------

    public FarragoRelImplementor(
        final FarragoPreparingStmt preparingStmt,
        RexBuilder rexBuilder)
    {
        super(
            rexBuilder,
            new UdfAwareOJRexImplementorTable(
                preparingStmt.getSession().getPersonality().getOJRexImplementorTable(
                    preparingStmt)));

        this.preparingStmt = preparingStmt;
        ojAssignableValue = OJClass.forClass(AssignableValue.class);
        ojBytePointer = OJClass.forClass(BytePointer.class);

        streamDefSet = new HashSet();
        nextParamId = 1;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Sets the MOFID of the foreign server associated with the
     * expression being implemented.
     *
     * @param serverMofId MOFID to set, or null to clear
     */
    public void setServerMofId(String serverMofId)
    {
        this.serverMofId = serverMofId;
    }

    // implement FarragoOJRexRelImplementor
    public String getServerMofId()
    {
        return serverMofId;
    }

    // implement FennelRelImplementor
    public FarragoRepos getRepos()
    {
        return preparingStmt.getRepos();
    }

    public int allocateDynamicParam()
    {
        return nextParamId++;
    }

    // implement FennelRelImplementor
    public FemExecutionStreamDef visitFennelChild(FennelRel rel)
    {
        FemExecutionStreamDef streamDef = rel.toStreamDef(this);
        registerRelStreamDef(streamDef, rel, null);
        return streamDef;
    }

    /**
     * Override method to deal with the possibility that we are being called
     * from a {@link FennelRel} via our {@link FennelRelImplementor}
     * interface.
     */
    public Object visitChildInternal(RelNode child)
    {
        if (child instanceof FennelRel) {
            return ((FennelRel) child).implementFennelChild(this);
        }
        return super.visitChildInternal(child);
    }

    public Set getStreamDefSet()
    {
        return streamDefSet;
    }

    public FarragoPreparingStmt getPreparingStmt()
    {
        return preparingStmt;
    }

    // implement FennelRelImplementor
    public void registerRelStreamDef(
        FemExecutionStreamDef streamDef,
        RelNode rel,
        RelDataType rowType)
    {
        if (rowType == null) {
            rowType = rel.getRowType();
        }
        registerStreamDef(streamDef, rel, rowType);
    }

    // implement FennelRelImplementor
    public void addDataFlowFromProducerToConsumer(
        FemExecutionStreamDef producer,
        FemExecutionStreamDef consumer)
    {
        FemExecStreamDataFlow flow = getRepos().newFemExecStreamDataFlow();
        producer.getOutputFlow().add(flow);
        consumer.getInputFlow().add(flow);
    }
    
    protected FemTupleDescriptor computeStreamDefOutputDesc(RelDataType rowType)
    {
        return FennelRelUtil.createTupleDescriptorFromRowType(
            preparingStmt.getRepos(),
            preparingStmt.getTypeFactory(),
            rowType);
    }

    private void registerStreamDef(
        FemExecutionStreamDef streamDef,
        RelNode rel,
        RelDataType rowType)
    {
        if (streamDef.getName() != null) {
            // already registered
            return;
        }

        String streamName = getStreamGlobalName(streamDef, rel);
        streamDef.setName(streamName);
        streamDefSet.add(streamDef);

        // REVIEW jvs 15-Nov-2004:  This is dangerous because rowType
        // may not be correct all the way down.
        if (streamDef.getOutputDesc() == null) {
            streamDef.setOutputDesc(computeStreamDefOutputDesc(rowType));
        }
        // recursively ensure all inputs have also been registered
        for (Object obj : streamDef.getInputFlow()) {
            FemExecStreamDataFlow flow = (FemExecStreamDataFlow) obj;
            FemExecutionStreamDef producer = flow.getProducer();
            registerStreamDef(producer, null, rowType);
        }
    }

    /**
     * Constructs a globally unique name for an execution stream.  This name is
     * used to label and find C++ ExecStreams.
     *
     * @param streamDef stream definition
     *
     * @param rel rel which generated stream definition, or null if none
     *
     * @return global name for stream
     */
    public String getStreamGlobalName(
        FemExecutionStreamDef streamDef,
        RelNode rel)
    {
        String streamName;
        if (rel != null) {
            // correlate stream name with rel which produced it
            streamName = rel.getRelTypeName() + "#" + rel.getId();
        } else {
            // anonymous stream
            streamName =
                ReflectUtil.getUnqualifiedClassName(streamDef.getClass());
        }

        // make sure stream names are globally unique
        streamName = streamName + ":" + JmiUtil.getObjectId(streamDef);
        return streamName;
    }

    // implement JavaRelImplementor
    public Variable getConnectionVariable()
    {
        return preparingStmt.getConnectionVariable();
    }

    // override JavaRelImplementor
    protected RexToOJTranslator newTranslator(RelNode rel)
    {
        // NOTE jvs 14-June-2004:  since we aren't given stmtList/memberList,
        // this translator is not usable for actual code generation, but
        // it's sufficient for use in TranslationTester, which is
        // currently the only caller
        return new FarragoRexToOJTranslator(
            preparingStmt.getRepos(),
            this, rel, implementorTable,
            null, null, null);
    }

    // override JavaRelImplementor
    public RexToOJTranslator newStmtTranslator(
        JavaRel rel,
        StatementList stmtList,
        MemberDeclarationList memberList)
    {
        return new FarragoRexToOJTranslator(
            preparingStmt.getRepos(),
            this, rel, implementorTable,
            stmtList, memberList, null);
    }

    // override JavaRelImplementor
    public Expression implementRoot(JavaRel rel)
    {
        Expression exp = super.implementRoot(rel);
        preparingStmt.prepareForCompilation();
        return exp;
    }

    /**
     * An operator implementor table which knows about UDF's.
     */
    private static class UdfAwareOJRexImplementorTable
        implements OJRexImplementorTable
    {
        private final OJRexImplementorTable delegate;

        public UdfAwareOJRexImplementorTable(OJRexImplementorTable delegate)
        {
            this.delegate = delegate;
        }

        public OJRexImplementor get(SqlOperator op)
        {
            if (op instanceof FarragoUserDefinedRoutine) {
                return (OJRexImplementor) op;
            } else {
                return delegate.get(op);
            }
        }

        public OJAggImplementor get(Aggregation aggregation)
        {
            return delegate.get(aggregation);
        }
    }

    /**
     * Definition for a lump of code which is to accept bindings at runtime.
     * The generated code will implement the {@link FarragoTransform}
     * interface.
     */
    public interface TransformDef
    {
        /**
         * Defines a port.
         *
         * @pre port.getOrdinal() == getPorts().size()
         */
        void definePort(PortDef port);

        /**
         * Returns a collection of {@link PortDef} objects.
         */
        Collection getPorts();

        /**
         * The declaration of the class which is being generated.
         * The class must implement the {@link FarragoTransform} interface.
         */
        ClassDeclaration getClassDecl();
    }

    /**
     * Definition of a port belonging to a {@link TransformDef}.
     * A port is a point at which data enters a transform from, or leaves a
     * transform to, an execution object.
     */
    public interface PortDef
    {
        /**
         * Returns the name of the member variable which holds the binding.
         */
        Variable getBindingVariable();

        /**
         * Converts this port to a stream definition. Subsequent calls must
         * return the same stream definition.
         */
        FemExecutionStreamDef getStreamDef(
            FennelRelImplementor implementor);

        /**
         * Returns the expression to bind this port. Must not be called before
         * {@link #getStreamDef(FennelRelImplementor)} has been called.
         *
         * <p>Typical code:
         * <blockquote><code><pre>
         * final Binding binding = bindings[0];
         * final Port port = binding.getPort(this);
         * final Object bind = binding.getObjectToBind(this);
         * final Object bound = port.bind(bind);
         * </pre></code></blockquote>
         *
         * @param implementor
         */
        Expression getExpr(FarragoRelImplementor implementor);

        void addInitializationCode(FarragoRelImplementor implementor);

        /**
         * Returns the port's ordinal within its transform.
         */
        int getOrdinal();
    }

    /**
     * Default implementation for {@link PortDef}.
     */
    public static abstract class PortDefImpl implements PortDef
    {
        private final TransformDef transform;
        protected final int portOrdinal;
        private final boolean input;
        private FemExecutionStreamDef streamDef;

        public PortDefImpl(
            TransformDef transform,
            int portOrdinal,
            boolean isInput)
        {
            this.transform = transform;
            this.portOrdinal = portOrdinal;
            this.input = isInput;
        }

        public FemExecutionStreamDef getStreamDef(
            FennelRelImplementor implementor)
        {
            if (streamDef == null) {
                streamDef = toStreamDef(implementor);
                assert streamDef != null;
            }
            return streamDef;
        }

        /**
         * Creates a stream definition.
         */
        protected abstract FemExecutionStreamDef toStreamDef(
            FennelRelImplementor implementor);

        public int getOrdinal()
        {
            return portOrdinal;
        }

        public void addInitializationCode(FarragoRelImplementor implementor)
        {
            // do nothing

            // Typical initialization code:
            //            final Port port = binding.getPort(this);
            //            final Object bind = binding.getObjectToBind(this);
            //            final Object bound = port.bind(bind);
        }

        public Variable getBindingVariable()
        {
            return new Variable("port" + portOrdinal);
        }
    }
}

// End FarragoRelImplementor.java
