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
 * BitReference represents a bit which can be accessed by marshalling and
 * unmarshalling. Could be a null indicator, could be a real bit value.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface BitReference
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Sets the bit value referenced by this.
     *
     * @param bit new value
     */
    public void setBit(boolean bit);

    /**
     * @return the bit value referenced by this
     */
    public boolean getBit();
}

// End BitReference.java
