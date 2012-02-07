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
package net.sf.farrago.fennel.tuple;

import java.io.*;

import java.util.*;


/**
 * FennelTupleDescriptor provides the metadata describing a tuple. This is used
 * in conjunction with FennelTupleAccessor objects to marshall and unmarshall
 * data into FennelTupleData objects from external formats. This class is JDK
 * 1.4 compatible.
 *
 * @author Mike Bennett
 * @version $Id$
 */
public class FennelTupleDescriptor
    implements Serializable
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * SerialVersionUID created with JDK 1.5 serialver tool.
     */
    private static final long serialVersionUID = -7075506007273800588L;

    //~ Instance fields --------------------------------------------------------

    /**
     * a collection of the FennelTupleAttributeDescriptor objects we're keeping.
     */
    private final List attrs = new ArrayList();

    //~ Constructors -----------------------------------------------------------

    /**
     * default constructor
     */
    public FennelTupleDescriptor()
    {
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the number of attributes we are holding.
     */
    public int getAttrCount()
    {
        return attrs.size();
    }

    /**
     * Gets an FennelTupleAttributeDescriptor given an ordinal index.
     */
    public FennelTupleAttributeDescriptor getAttr(int i)
    {
        return (FennelTupleAttributeDescriptor) attrs.get(i);
    }

    /**
     * Adds a new FennelTupleAttributeDescriptor.
     *
     * @return the index where it was added
     */
    public int add(FennelTupleAttributeDescriptor newDesc)
    {
        int ndx = attrs.size();
        attrs.add(newDesc);
        return ndx;
    }

    /**
     * Indicates if any descriptors we're keeping might contain nulls.
     */
    public boolean containsNullable()
    {
        int i;
        for (i = 0; i < attrs.size(); ++i) {
            if (getAttr(i).isNullable) {
                return true;
            }
        }
        return false;
    }
}

// End FennelTupleDescriptor.java
