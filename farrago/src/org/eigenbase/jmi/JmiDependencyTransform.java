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

import java.util.*;

import javax.jmi.reflect.*;


/**
 * JmiDependencyTransform defines a transformation for use in constructing a
 * {@link JmiDependencyGraph}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface JmiDependencyTransform
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Searches a collection of candidate objects, returning only those which
     * are reachable via links corresponding to mapped incoming model edges to a
     * target object.
     *
     * @param target object to which incoming links are to be found
     * @param candidates candidate source objects
     * @param mapping mapping filter for links
     *
     * @return matching candidates
     */
    public Collection<RefObject> getSourceNeighbors(
        RefObject target,
        Collection<RefObject> candidates,
        JmiAssocMapping mapping);

    /**
     * @return true if self-loops in the dependency graph should be allowed;
     * false to automatically filter them out
     */
    public boolean shouldProduceSelfLoops();

    /**
     * @return a comparator which can be used for breaking ties in ordering, or
     * null if no tie-breaking is desired (tie-breaking provides stability
     * during diff-based testing, but adds processing overhead)
     */
    public Comparator<RefBaseObject> getTieBreaker();
}

// End JmiDependencyTransform.java
