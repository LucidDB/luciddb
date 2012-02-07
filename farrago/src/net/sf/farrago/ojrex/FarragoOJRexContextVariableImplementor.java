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
 * Implements Farrago specifics of {@link OJRexImplementor} for context
 * variables such as "USER" and "SESSION_USER".
 *
 * <p>For example, "USER" becomes "connection.getContextVariable_USER()".
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class FarragoOJRexContextVariableImplementor
    implements OJRexImplementor
{
    //~ Instance fields --------------------------------------------------------

    private final String name;

    //~ Constructors -----------------------------------------------------------

    public FarragoOJRexContextVariableImplementor(String name)
    {
        this.name = name;
    }

    //~ Methods ----------------------------------------------------------------

    public Expression implement(
        RexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        FarragoRexToOJTranslator farragoTranslator =
            (FarragoRexToOJTranslator) translator;
        return farragoTranslator.convertVariable(
            call.getType(),
            "getContextVariable_" + name,
            new ExpressionList());
    }

    public boolean canImplement(RexCall call)
    {
        return true;
    }
}

// End FarragoOJRexContextVariableImplementor.java
