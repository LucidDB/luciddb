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


/**
 * FennelTupleAttributeDescriptor holds metadata describing a particular entry
 * in a tuple. These are contained in a FennelTupleDescriptor object to describe
 * the layout of a tuple. This class is JDK 1.4 compatible.
 *
 * @author Mike Bennett
 * @version $Id$
 */
public class FennelTupleAttributeDescriptor
    implements Serializable
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * SerialVersionUID created with JDK 1.5 serialver tool.
     */
    private static final long serialVersionUID = -4582426550989158154L;

    //~ Instance fields --------------------------------------------------------

    /**
     * the FennelStoredTypeDescriptor of this attribute.
     */
    public FennelStoredTypeDescriptor typeDescriptor;

    /**
     * is this attribute nullable?
     */
    public boolean isNullable;

    /**
     * the amount of storage, in bytes, taken by this attribute.
     */
    public int storageSize;

    //~ Constructors -----------------------------------------------------------

    /**
     * Default constructor -- shouldn't be used in normal situations.
     */
    public FennelTupleAttributeDescriptor()
    {
        isNullable = false;
        storageSize = 0;
    }

    /**
     * Normal constructor
     */
    public FennelTupleAttributeDescriptor(
        FennelStoredTypeDescriptor typeDescriptor,
        boolean isNullable,
        int storageSizeInit)
    {
        this.typeDescriptor = typeDescriptor;
        this.isNullable = isNullable;
        if (storageSizeInit > 0) {
            int fixedSize = typeDescriptor.getFixedByteCount();
            assert ((fixedSize == 0) || (fixedSize == storageSizeInit));
            storageSize = storageSizeInit;
        } else {
            storageSize = typeDescriptor.getFixedByteCount();
        }
    }
}

// End FennelTupleAttributeDescriptor.java
