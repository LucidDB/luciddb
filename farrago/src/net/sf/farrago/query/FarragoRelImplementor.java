/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2003-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
import java.util.List;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.ojrex.*;
import net.sf.farrago.type.runtime.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.jmi.JmiObjUtil;
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
public class FarragoRelImplementor
    extends JavaRelImplementor
    implements FennelRelImplementor,
        FarragoOJRexRelImplementor
{
    //~ Instance fields --------------------------------------------------------

    FarragoPreparingStmt preparingStmt;
    OJClass ojAssignableValue;
    OJClass ojBytePointer;
    private Set<FemExecutionStreamDef> streamDefSet;
    private String serverMofId;
    private long nextRelParamId;
    private int nextDynamicParamId;

    // An ordered list representing the path from the root RelNode to the
    // current child RelNode being visited in the execution graph.
    private List<RelPathEntry> currRelPathList;

    // Mapping from the different RelPathEntry lists to the dynamic parameters
    // accessible from the leaf RelNode in that RelPathEntry list
    private Map<List<RelPathEntry>, RelScope> relPathScopeMap;

    private int nextTransformId;

    /**
     * List of ClassDeclarations representing generated Java code not directly
     * linked to the plan's root rel node.
     */
    private List<ClassDeclaration> transformDeclarations;

    //~ Constructors -----------------------------------------------------------

    public FarragoRelImplementor(
        final FarragoPreparingStmt preparingStmt,
        RexBuilder rexBuilder)
    {
        super(
            rexBuilder,
            new UdfAwareOJRexImplementorTable(
                preparingStmt.getSession().getPersonality()
                             .getOJRexImplementorTable(
                                 preparingStmt)));

        this.preparingStmt = preparingStmt;
        ojAssignableValue = OJClass.forClass(AssignableValue.class);
        ojBytePointer = OJClass.forClass(BytePointer.class);

        streamDefSet = new HashSet<FemExecutionStreamDef>();
        currRelPathList = new LinkedList<RelPathEntry>();
        relPathScopeMap = new HashMap<List<RelPathEntry>, RelScope>();
        nextRelParamId = 1;

        // REVIEW jvs 22-Mar-2006:  does this match how user-level
        // dynamic params get mapped into Fennel?
        nextDynamicParamId =
            preparingStmt.getSqlToRelConverter().getDynamicParamCount() + 1;
        nextTransformId = 1;
        transformDeclarations = new ArrayList<ClassDeclaration>();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets the MOFID of the foreign server associated with the expression being
     * implemented.
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

    public FennelRelParamId allocateRelParamId()
    {
        return new FennelRelParamId(nextRelParamId++);
    }

    public FennelDynamicParamId translateParamId(FennelRelParamId relParamId)
    {
        return
            translateParamId(
                relParamId,
                null,
                FennelDynamicParamId.StreamType.UNKNOWN);
    }

    public FennelDynamicParamId translateParamId(
        FennelRelParamId relParamId,
        FemExecutionStreamDef streamDef,
        FennelDynamicParamId.StreamType streamType)
    {
        // FennelDynamicParamid == 0 represents an unused parameter
        if (relParamId == null) {
            return new FennelDynamicParamId(0);
        }

        // Check for an existing translation by first looking for
        // currRelPathList in relPathScopeMap.  If we can't find it,
        // then progressively reduce the entries in the path list by one
        // entry, until we empty the list.
        List<RelPathEntry> targetPathList =
            new LinkedList<RelPathEntry>(currRelPathList);
        do {
            RelScope scope = relPathScopeMap.get(targetPathList);
            if (scope != null) {
                FennelDynamicParamId dynamicParamId =
                    scope.paramMap.get(relParamId);
                if (dynamicParamId != null) {
                    dynamicParamId.associateStream(streamDef, streamType);
                    return dynamicParamId;
                }
            }
            targetPathList.remove(0);
        } while (!targetPathList.isEmpty());

        // None found:  make up a new one and add it to current scope.
        FennelDynamicParamId dynamicParamId =
            new FennelDynamicParamId(
                nextDynamicParamId++,
                streamDef,
                streamType);
        RelScope scope = relPathScopeMap.get(currRelPathList);
        if (scope == null) {
            scope = new RelScope();
            // Make a copy of the current list to store in the map,
            // since we'll continue adding and removing elements from the
            // current list
            List<RelPathEntry> pathList =
                new LinkedList<RelPathEntry>(currRelPathList);
            relPathScopeMap.put(pathList, scope);
        }
        scope.paramMap.put(relParamId, dynamicParamId);
        return dynamicParamId;
    }

    // implement FennelRelImplementor
    public void setErrorRecordType(
        FennelRel rel,
        FemExecutionStreamDef streamDef,
        RelDataType errorType)
    {
        // retrieve the stream name early to perform a
        // (stream => error record type) mapping by stream name
        String streamName = getStreamGlobalName(streamDef, rel);
        streamDef.setName(streamName);

        FarragoPreparingStmt stmt = FennelRelUtil.getPreparingStmt(rel);
        stmt.mapResultSetType(streamName, errorType);
    }

    // implement FennelRelImplementor
    public FemExecutionStreamDef visitFennelChild(FennelRel rel, int ordinal)
    {
        FemExecutionStreamDef streamDef = toStreamDefImpl(rel, ordinal);
        registerRelStreamDef(streamDef, rel, null);
        return streamDef;
    }

    public List<RelPathEntry> getRelPathEntry()
    {
    	return currRelPathList;
    }
    
    /**
     * Adds a RelPathEntry corresponding to a new RelNode to the current
     * RelPathEntry list
     *
     * @param rel the new RelNode
     * @param ordinal the input position of the RelNode
     */
    protected void addRelPathEntry(RelNode rel, int ordinal)
    {
        RelPathEntry pathEntry = new RelPathEntry(rel, ordinal);
        currRelPathList.add(0, pathEntry);
    }

    /**
     * Removes the RelPathEntry corresponding to the current RelNode being
     * visited from the current RelPathEntry list
     */
    protected void removeRelPathEntry()
    {
        currRelPathList.remove(0);
    }

    /**
     * Converts a {@link FennelRel} to a {@link FemExecutionStreamDef}, and
     * prints context if anything goes wrong.
     *
     * <p>This method is final: derived classes should not add extra
     * functionality.
     *
     * @param rel Relational expression
     * @param ordinal input position of the relational expression for its
     * parent
     *
     * @return Plan
     */
    protected final FemExecutionStreamDef toStreamDefImpl(
        FennelRel rel,
        int ordinal)
    {
        addRelPathEntry(rel, ordinal);

        FemExecutionStreamDef streamDef;
        try {
            streamDef = rel.toStreamDef(this);
        } catch (Throwable e) {
            throw Util.newInternal(
                e,
                "Error occurred while translating relational expression "
                + rel + " to a plan");
        }
        removeRelPathEntry();
        
        return streamDef;
    }

    /**
     * Override method to deal with the possibility that we are being called
     * from a {@link FennelRel} via our {@link FennelRelImplementor} interface.
     */
    public Object visitChildInternal(RelNode child, int ordinal)
    {
        addRelPathEntry(child, ordinal);

        Object retObj;
        if (child instanceof FennelRel) {
            retObj = ((FennelRel) child).implementFennelChild(this);
        } else {
            retObj = super.visitChildInternal(child, ordinal);
        }
        removeRelPathEntry();

        return retObj;
    }

    public Set<FemExecutionStreamDef> getStreamDefSet()
    {
        return streamDefSet;
    }

    public List<ClassDeclaration> getTransforms()
    {
        return Collections.unmodifiableList(transformDeclarations);
    }

    public void addTransform(ClassDeclaration transform)
    {
        transformDeclarations.add(transform);
    }

    public int allocateTransform()
    {
        return nextTransformId++;
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
        addDataFlowFromProducerToConsumer(producer, consumer, false);
    }

    // implement FennelRelImplementor
    public void addDataFlowFromProducerToConsumer(
        FemExecutionStreamDef producer,
        FemExecutionStreamDef consumer,
        boolean implicit)
    {
        FemExecStreamDataFlow flow = getRepos().newFemExecStreamDataFlow();
        flow.setImplicit(implicit);
        producer.getOutputFlow().add(flow);
        consumer.getInputFlow().add(flow);
    }

    protected FemTupleDescriptor computeStreamDefOutputDesc(
        RelDataType rowType)
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
        if (streamDefSet.contains(streamDef)) {
            // already registered
            return;
        }

        if (streamDef.getName() == null) {
            String streamName = getStreamGlobalName(streamDef, rel);
            streamDef.setName(streamName);
        }
        streamDefSet.add(streamDef);

        // REVIEW jvs 15-Nov-2004:  This is dangerous because rowType
        // may not be correct all the way down.
        if (streamDef.getOutputDesc() == null) {
            streamDef.setOutputDesc(computeStreamDefOutputDesc(rowType));
        }

        // recursively ensure all inputs have also been registered
        for (FemExecStreamDataFlow flow : streamDef.getInputFlow()) {
            FemExecutionStreamDef producer = flow.getProducer();
            registerStreamDef(producer, null, rowType);
        }
    }

    /**
     * Constructs a globally unique name for an execution stream. This name is
     * used to label and find C++ ExecStreams.
     *
     * @param streamDef stream definition
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
            streamName = rel.getRelTypeName() + ".#" + rel.getId();
        } else {
            // anonymous stream
            streamName =
                ReflectUtil.getUnqualifiedClassName(streamDef.getClass())
                + ".";
        }

        // make sure stream names are globally unique
        streamName = streamName + ":" + JmiObjUtil.getObjectId(streamDef);
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
            this,
            rel,
            implementorTable,
            null,
            null,
            null);
    }

    // override JavaRelImplementor
    public RexToOJTranslator newStmtTranslator(
        JavaRel rel,
        StatementList stmtList,
        MemberDeclarationList memberList)
    {
        return new FarragoRexToOJTranslator(
            preparingStmt.getRepos(),
            this,
            rel,
            implementorTable,
            stmtList,
            memberList,
            null);
    }

    // override JavaRelImplementor
    public Expression implementRoot(JavaRel rel)
    {
        Expression exp = super.implementRoot(rel);
        preparingStmt.prepareForCompilation();
        return exp;
    }

    //~ Inner Classes ----------------------------------------------------------

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
     * RelPathEntry keeps track of a RelNode and its input position within
     * that node's parent RelNode in the execution stream graph.
     */
    public static class RelPathEntry
    {
        final RelNode relNode;
        final int ordinal;

        RelPathEntry(RelNode relNode, int ordinal)
        {
            this.relNode = relNode;
            this.ordinal = ordinal;
        }

        public int hashCode()
        {
            return relNode.hashCode() + ordinal;
        }

        public boolean equals(Object o)
        {
            RelPathEntry relPathEntry = (RelPathEntry) o;
            return
                (relPathEntry.relNode == relNode &&
                    relPathEntry.ordinal == ordinal);
        }

        public String toString()
        {
            return "RelPathEntry(" + ordinal + ", "
                + "rel#" + relNode.getId() + ":"
                + relNode.getRelTypeName() + ")";
        }
    }

    private static class RelScope
    {
        final Map<FennelRelParamId, FennelDynamicParamId> paramMap;

        RelScope()
        {
            paramMap = new HashMap<FennelRelParamId, FennelDynamicParamId>();
        }
    }
}

// End FarragoRelImplementor.java
