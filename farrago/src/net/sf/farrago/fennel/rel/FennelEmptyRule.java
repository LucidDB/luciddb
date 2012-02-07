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
package net.sf.farrago.fennel.rel;

import java.util.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;


/**
 * FennelEmptyRule provides an implementation for {@link
 * org.eigenbase.rel.EmptyRel} in terms of {@link
 * net.sf.farrago.fennel.rel.FennelValuesRel}.
 *
 * @author jhyde
 * @version $Id$
 */
public class FennelEmptyRule
    extends ConverterRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Singleton instance of this rule.
     */
    public static final FennelEmptyRule instance = new FennelEmptyRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelEmptyRule.
     */
    private FennelEmptyRule()
    {
        super(
            EmptyRel.class,
            CallingConvention.NONE,
            FennelRel.FENNEL_EXEC_CONVENTION,
            "FennelEmptyRule");
    }

    //~ Methods ----------------------------------------------------------------

    // implement ConverterRule
    public RelNode convert(RelNode rel)
    {
        EmptyRel valuesRel = (EmptyRel) rel;

        return new FennelValuesRel(
            valuesRel.getCluster(),
            valuesRel.getRowType(),
            Collections.<List<RexLiteral>>emptyList());
    }
}

// End FennelEmptyRule.java
