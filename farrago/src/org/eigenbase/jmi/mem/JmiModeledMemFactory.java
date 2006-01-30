/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package org.eigenbase.jmi.mem;

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

/**
 * JmiModeledMemFactory augments {@link JmiMemFactory} with information
 * from an underlying metamodel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class JmiModeledMemFactory extends JmiMemFactory
{
    public JmiModeledMemFactory(JmiModelGraph modelGraph)
    {
        try {
            for (Object edgeObj : modelGraph.getAssocGraph().edgeSet()) {
                JmiAssocEdge edge = (JmiAssocEdge) edgeObj;
                Class sourceInterface = JmiObjUtil.getJavaInterfaceForRefClass(
                    ((JmiClassVertex) edge.getSource()).getRefClass());
                String targetAttrName = parseGetter(
                    JmiObjUtil.getAccessorName(edge.getTargetEnd()));
                boolean targetMany =
                    (edge.getTargetEnd().getMultiplicity().getUpper() != 1);
                
                Class targetInterface = JmiObjUtil.getJavaInterfaceForRefClass(
                    ((JmiClassVertex) edge.getTarget()).getRefClass());
                String sourceAttrName = parseGetter(
                    JmiObjUtil.getAccessorName(edge.getSourceEnd()));
                boolean sourceMany =
                    (edge.getSourceEnd().getMultiplicity().getUpper() != 1);
                
                createRelationship(
                    sourceInterface,
                    targetAttrName,
                    targetMany,
                    targetInterface,
                    sourceAttrName,
                    sourceMany);
            }
        } catch (ClassNotFoundException ex) {
            throw Util.newInternal(ex);
        }
    }
}

// End JmiModeledMemFactory.java
