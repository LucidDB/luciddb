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

import net.sf.farrago.type.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.reltype.*;


/**
 * Utility functions for generating OpenJava expressions.
 *
 * @author John Pham
 * @version $Id$
 */
public class FarragoOJRexUtil
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a field access, as in expr.[value] for a primitive
     */
    public static Expression getValueAccessExpression(
        FarragoRexToOJTranslator translator,
        RelDataType type,
        Expression expr)
    {
        FarragoTypeFactory factory =
            (FarragoTypeFactory) translator.getTypeFactory();
        return factory.getValueAccessExpression(type, expr);
    }
}

// End FarragoOJRexUtil.java
