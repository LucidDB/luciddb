/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
     * @param ordinal input position of the relational expression for its
     * parent
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
     * @param streamDef the stream that either produces or consumes the
     * dynamic parameter; or null if we don't need to keep track of that
     * information
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
}

// End FennelRelImplementor.java
