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
 * JmiAssocMapping enumerates the possible ways an association edge can be
 * mapped when a JMI graph is being transformed.
 *
 * @author John Sichi
 * @version $Id$
 */
public enum JmiAssocMapping
{
    /**
     * The association should be left out of the transformed graph.
     */
    REMOVAL,

    /**
     * The association edge should be preserved in the transformed graph.
     */
    COPY,

    /**
     * The association edge should be preserved in the transformed graph, but
     * its direction should be reversed.
     */
    REVERSAL,

    /**
     * The two ends of the association should be contracted into one vertex in
     * the transformed graph.
     */
    CONTRACTION,

    /**
     * The association edge should be interpreted as a hierarchical structure to
     * be superimposed on the graph, with the source as parent and the target as
     * child.
     */
    HIERARCHY
}

// End JmiAssocMapping.java
