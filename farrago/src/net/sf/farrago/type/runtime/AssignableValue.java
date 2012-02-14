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
package net.sf.farrago.type.runtime;

/**
 * The AssignableValue interface represents a writable SQL value of
 * non-primitive type at runtime.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface AssignableValue
    extends DataValue
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Name of method implementing assignment operator.
     */
    public static final String ASSIGNMENT_METHOD_NAME = "assignFrom";

    //~ Methods ----------------------------------------------------------------

    /**
     * Assigns value from an Object.
     *
     * @param obj value to assign, or null to set null
     */
    public void assignFrom(Object obj);
}

// End AssignableValue.java
