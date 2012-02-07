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

import javax.jmi.model.*;

import org.jgrapht.graph.*;


/**
 * JmiInheritanceEdge represents an inheritance relationship in a JMI model. The
 * source vertex is the superclass and the target vertex is the subclass.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiInheritanceEdge
    extends DefaultEdge
{
    //~ Instance fields --------------------------------------------------------

    private final JmiClassVertex superClass;
    private final JmiClassVertex subClass;

    //~ Constructors -----------------------------------------------------------

    JmiInheritanceEdge(
        JmiClassVertex superClass,
        JmiClassVertex subClass)
    {
        this.superClass = superClass;
        this.subClass = subClass;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the vertex representing the superclass
     */
    public JmiClassVertex getSuperClass()
    {
        return superClass;
    }

    /**
     * @return the vertex representing the subclass
     */
    public JmiClassVertex getSubClass()
    {
        return subClass;
    }

    // implement Object
    public String toString()
    {
        return getSuperClass().toString() + "_generalizes_" + getSubClass();
    }
}

// End JmiInheritanceEdge.java
