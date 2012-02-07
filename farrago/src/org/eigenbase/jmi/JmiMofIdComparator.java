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
 * JmiMofIdComparator implements the {@link Comparator} interface by comparing
 * pairs of {@link RefBaseObject} instances according to their MOFID attribute.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiMofIdComparator
    implements Comparator<RefBaseObject>
{
    //~ Static fields/initializers ---------------------------------------------

    public static final JmiMofIdComparator instance = new JmiMofIdComparator();

    //~ Constructors -----------------------------------------------------------

    private JmiMofIdComparator()
    {
    }

    //~ Methods ----------------------------------------------------------------

    public int compare(RefBaseObject o1, RefBaseObject o2)
    {
        return o1.refMofId().compareTo(o2.refMofId());
    }

    public boolean equals(Object obj)
    {
        return (obj instanceof JmiMofIdComparator);
    }
}

// End JmiMofIdComparator.java
