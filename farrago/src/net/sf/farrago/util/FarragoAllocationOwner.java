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
package net.sf.farrago.util;

import org.eigenbase.util.*;


/**
 * FarragoAllocationOwner represents an object which can take ownership of
 * FarragoAllocations and guarantee that they will be cleaned up correctly when
 * its own closeAllocation() is called.
 *
 * <p>REVIEW: SWZ: 2/22/2006: New code should use ClosableAllocationOwner
 * directly when possible. Eventually remove this interface and replace all
 * usages with ClosableAllocationOwner.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoAllocationOwner
    extends ClosableAllocationOwner,
        FarragoAllocation
{
}

// End FarragoAllocationOwner.java
