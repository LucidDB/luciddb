/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * Callback used to hold state while converting a tree of {@link FennelRel}
 * objects into a plan consisting of {@link FemExecutionStreamDef} objects.
 *
 * @author jhyde
 * @version $Id$
 * @see FarragoRelImplementor
 * @since May 24, 2004
 */
public interface FennelRelImplementor
    extends RelImplementor
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Converts a relational expression into a plan by calling its {@link
     * FennelRel#toStreamDef} method.
     *
     * @param rel the relational expression
     * @param ordinal input position of the relational expression for its parent
     */
    public FemExecutionStreamDef visitFennelChild(FennelRel rel, int ordinal);

    /**
     * Registers a new stream definition. Normally, it is not necessary to call
     * this method explicitly; it happens automatically as part of {@link
     * #visitFennelChild}. However, this is not true for non-tree stream graphs.
     * For streams with multiple parents, this method must be called for streams
     * not returned from {@link #visitFennelChild}.
     *
     * @param streamDef new stream definition
     * @param rel RelNode which stream implements
     * @param rowType row type for stream, or null to use rel's row type
     */
    public void registerRelStreamDef(
        FemExecutionStreamDef streamDef,
        RelNode rel,
        RelDataType rowType);

    /**
     * Adds a new explicit dataflow edge between two existing stream
     * definitions. See the three-arg version for details.
     *
     * <p>NOTE jvs 14-Nov-2005: I gave this method a long name so that it
     * wouldn't be necessary to guess the direction when reading code that uses
     * it.
     *
     * @param producer the upstream node of the dataflow
     * @param consumer the downstream node of the dataflow
     */
    public void addDataFlowFromProducerToConsumer(
        FemExecutionStreamDef producer,
        FemExecutionStreamDef consumer);

    /**
     * Adds a new dataflow edge between two existing stream definitions. In
     * cases where a stream has multiple inputs or outputs, order may be
     * significant, in which case it is the caller's responsibility to add the
     * flows in the desired order.
     *
     * @param producer the upstream node of the dataflow
     * @param consumer the downstream node of the dataflow
     * @param implicit false if this is an explicit dataflow edge between two
     * ExecStreams; true if it represents implicit dataflow via a UDX reading
     * from a cursor
     */
    public void addDataFlowFromProducerToConsumer(
        FemExecutionStreamDef producer,
        FemExecutionStreamDef consumer,
        boolean implicit);

    /**
     * Returns the repository.
     */
    public FarragoRepos getRepos();

    /**
     * Reserves a Fennel dynamic parameter. The reserving rel can use {@link
     * #translateParamId(FennelRelParamId)} later as part of its toStreamDef
     * implementation to convert this into a final {@link FennelDynamicParamId},
     * which can then be referenced from stream definitions.
     *
     * @return parameter reservation ID
     */
    public FennelRelParamId allocateRelParamId();

    /**
     * Translates a {@link FennelRelParamId} into a {@link FennelDynamicParamId}
     * based on the current scope.
     *
     * @param relParamId reserved ID to be translated
     *
     * @return physical ID to use in final plan
     */
    public FennelDynamicParamId translateParamId(
        FennelRelParamId relParamId);

    /**
     * Translates a {@link FennelRelParamId} into a {@link FennelDynamicParamId}
     * based on the current scope.
     *
     * @param relParamId reserved ID to be translated
     * @param streamDef the stream that either produces or consumes the dynamic
     * parameter; or null if we don't need to keep track of that information
     * @param streamType whether the streamDef produces or consumes the dynamic
     * parameter
     *
     * @return physical ID to use in final plan
     */
    public FennelDynamicParamId translateParamId(
        FennelRelParamId relParamId,
        FemExecutionStreamDef streamDef,
        FennelDynamicParamId.StreamType streamType);

    /**
     * Sets the format of error records for an execution stream.
     *
     * @param rel relation to which the execution stream belongs
     * @param streamDef stream definition of stream that may produce errors
     * @param errorType row type of error records produced by the stream
     */
    public void setErrorRecordType(
        FennelRel rel,
        FemExecutionStreamDef streamDef,
        RelDataType errorType);

    /**
     * Returns the list of stream definitions that have been registered for
     * a RelNode.
     *
     * @param rel the RelNode
     *
     * @return the list of registered stream definitions; null if no stream
     * definitions have been registered yet to the RelNode
     */
    public List<FemExecutionStreamDef> getRegisteredStreamDefs(RelNode rel);

    /**
     * Determines if this is the instance of a RelNode such that the instance
     * corresponds to the one at the time this method was first called.
     *
     * @param rel the RelNode
     *
     * @return true if the RelNode instance is the one encountered the first
     * time this method was called
     */
    public boolean isFirstTranslationInstance(RelNode rel);
}

// End FennelRelImplementor.java
