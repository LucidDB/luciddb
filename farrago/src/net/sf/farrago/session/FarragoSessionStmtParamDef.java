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
package net.sf.farrago.session;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.util.*;


/**
 * FarragoSessionStmtParamDef represents the definition of a dynamic parameter
 * used within a {@link FarragoSessionStmtContext}. Instances of
 * FarragoSessionStmtParamDef are created by a {@link
 * FarragoSessionStmtParamDefFactory} and are used to validate dynamic parameter
 * values.
 *
 * @author Stephan Zuercher
 * @version $Id$
 * @see FarragoSessionStmtContext#setDynamicParam(int, Object)
 */
public interface FarragoSessionStmtParamDef
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the name of this parameter.
     *
     * @return the name of this parameter.
     */
    String getParamName();

    /**
     * Returns the {@link RelDataType} of this parameter.
     *
     * @return the {@link RelDataType} of this parameter.
     */
    RelDataType getParamType();

    /**
     * Checks the type of a value, and throws an error if it is invalid or
     * cannot be converted to an acceptable type.
     *
     * @param value
     *
     * @return value if valid; an acceptable value if a conversion is available
     *
     * @throws EigenbaseException if value is invalid and cannot be converted
     */
    Object scrubValue(Object value)
        throws EigenbaseException;

    /**
     * Checks the type of a value, and throws an error if it is invalid or
     * cannot be converted to an acceptable type.
     *
     * @param value
     * @param cal Calendar to use
     *
     * @return value if valid; an acceptable value if a conversion is available
     *
     * @throws EigenbaseException if value is invalid and cannot be converted
     */
    Object scrubValue(Object value, Calendar cal)
        throws EigenbaseException;
}

// End FarragoSessionStmtParamDef.java
