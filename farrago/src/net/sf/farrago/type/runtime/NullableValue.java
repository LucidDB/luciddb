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
 * NullableValue is an interface representing a runtime holder for a nullable
 * object.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface NullableValue
    extends DataValue
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Name of accessor method for null indicator.
     */
    public static final String NULL_IND_ACCESSOR_NAME = "isNull";

    /**
     * Name of accessor method for null indicator.
     */
    public static final String NULL_IND_MUTATOR_NAME = "setNull";

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets whether or not the value is null. Note that once a value has been
     * set to null, its data should not be updated until the null state has been
     * cleared with a call to setNull(false).
     *
     * @param isNull true to set a null value; false to indicate a non-null
     * value
     */
    void setNull(boolean isNull);

    /**
     * @return whether the value has been set to null
     */
    boolean isNull();
}

// End NullableValue.java
