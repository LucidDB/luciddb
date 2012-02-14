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
package net.sf.farrago.ojrex;

import openjava.ptree.*;

import org.eigenbase.oj.rex.*;
import org.eigenbase.rex.*;


/**
 * FarragoOJRexImplementor refines {@link OJRexImplementor} to provide
 * Farrago-specific context.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoOJRexImplementor
    implements OJRexImplementor
{
    //~ Methods ----------------------------------------------------------------

    // implement OJRexImplementor
    public Expression implement(
        RexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        return implementFarrago(
            (FarragoRexToOJTranslator) translator,
            call,
            operands);
    }

    /**
     * Refined version of {@link OJRexImplementor#implement}.
     *
     * @param translator provides Farrago-specific translation context
     * @param call the call to be translated
     * @param operands call's operands, which have already been translated
     * independently
     */
    public abstract Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands);

    // implement OJRexImplementor
    public boolean canImplement(RexCall call)
    {
        if (RexUtil.requiresDecimalExpansion(call, true)) {
            return false;
        }

        // NOTE jvs 17-June-2004:  In general, we assume that if
        // an implementor is registered, it is capable of the
        // requested implementation independent of operands.
        // Implementors which need to check their operands
        // should override this method.
        return true;
    }
}

// End FarragoOJRexImplementor.java
