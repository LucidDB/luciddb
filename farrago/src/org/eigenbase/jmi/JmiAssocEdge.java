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

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import org.jgrapht.graph.*;


/**
 * JmiAssocEdge represents an association in a JMI model. The source vertex is
 * the class for the source end and the target vertex is the class for the
 * target end, where
 *
 * <ul>
 * <li>if an end is composite, it is the source end
 * <li>else if an end has multiplicity > 1, it is the target end if the other
 * end has multiplicity <= 1
 * <li>else if an end is ordered, it is the target end
 * <li>otherwise, the first end is the source end and the second end is the
 * target end
 * </ul>
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class JmiAssocEdge
    extends DefaultEdge
{
    //~ Instance fields --------------------------------------------------------

    private final Association mofAssoc;

    private final AssociationEnd [] mofAssocEnds;

    RefAssociation refAssoc;

    //~ Constructors -----------------------------------------------------------

    JmiAssocEdge(
        Association mofAssoc,
        AssociationEnd [] mofAssocEnds)
    {
        this.mofAssoc = mofAssoc;
        this.mofAssocEnds = mofAssocEnds;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the MOF association represented by this edge
     */
    public Association getMofAssoc()
    {
        return mofAssoc;
    }

    /**
     * @return the RefAssociation represented by this edge
     */
    public RefAssociation getRefAssoc()
    {
        return refAssoc;
    }

    /**
     * @return the AssociationEnd for the source end
     */
    public AssociationEnd getSourceEnd()
    {
        return getEnd(0);
    }

    /**
     * @return the AssociationEnd for the target end
     */
    public AssociationEnd getTargetEnd()
    {
        return getEnd(1);
    }

    /**
     * @return true iff source end is MOF "first end"
     */
    public boolean matchesMofDirection()
    {
        return getSourceEnd().equals(mofAssoc.getContents().get(0));
    }

    /**
     * Retrieves an end of the association.
     *
     * @param iEnd ordinal of end to get (0 for source, 1 for target)
     *
     * @return the requested AssociationEnd
     */
    public AssociationEnd getEnd(int iEnd)
    {
        return mofAssocEnds[iEnd];
    }

    // implement Object
    public String toString()
    {
        return mofAssocEnds[0].getType().getName() + ":"
            + mofAssocEnds[0].getName()
            + "_" + mofAssoc.getName() + "_"
            + mofAssocEnds[1].getType().getName() + ":"
            + mofAssocEnds[1].getName();
    }
}

// End JmiAssocEdge.java
