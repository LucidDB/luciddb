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
 * Special exception to flag a reference to an unvalidated dependency. When such
 * a dependency is detected (usually in the context of a cyclic definition), we
 * throw this exception to terminate processing of the current object. The
 * JmiChangeSet catches it and recovers, marking the object as needing another
 * try, and moves on to other objects. If validation reaches a fixpoint, it
 * means there is an object definition cycle (which is illegal).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiUnvalidatedDependencyException
    extends RuntimeException
{
}

// End JmiUnvalidatedDependencyException.java
