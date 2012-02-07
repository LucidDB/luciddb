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

import net.sf.farrago.fem.fennel.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * FennelRel defines the interface which must be implemented by any {@link
 * RelNode} corresponding to a C++ physical implementation conforming to the
 * fennel::ExecStream interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FennelRel
    extends RelNode
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Calling convention which transfers data as rows in Fennel tuple format
     * (implementations must conform to the fennel::ExecStream interface).
     */
    public static final CallingConvention FENNEL_EXEC_CONVENTION =
        new CallingConvention(
            "FENNEL_EXEC",
            CallingConvention.generateOrdinal(),
            FennelRel.class);

    //~ Methods ----------------------------------------------------------------

    /**
     * Converts this relational expression to {@link FemExecutionStreamDef}
     * form. In the process, the relational expression will almost certainly
     * call {@link FennelRelImplementor#visitFennelChild} on each of its
     * children.
     *
     * @param implementor for generating Java code
     *
     * @return generated FemExecutionStreamDef
     */
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor);

    /**
     * Visits this relational expression as part of the implementation process.
     * Fennel relational expressions are implemented in a two-phase process:
     * first call this method, then call {@link #toStreamDef}.
     */
    Object implementFennelChild(FennelRelImplementor implementor);

    /**
     * <p>TODO: jhyde, 2006/3/28: unify with {@link RelNode#getCollationList()}
     *
     * @return the sort order produced by this FennelRel, or an empty array if
     * the output is not guaranteed to be in any particular order.
     */
    public RelFieldCollation [] getCollations();
}

// End FennelRel.java
