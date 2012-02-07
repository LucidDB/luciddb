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
package org.eigenbase.jmi;

/**
 * JmiDeletionAction enumerates the possible actions to take when a
 * JmiDeletionRule applies.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public enum JmiDeletionAction
{
    /**
     * Prevent the original modification from taking place.
     */
    RESTRICT,

    /**
     * Break the association, possibly leaving the object on the other side
     * dangling.
     */
    INVALIDATE,

    /**
     * Cascade the modification recursively to the object on the other side of
     * the association.
     */
    CASCADE
}

// End JmiDeletionAction.java
