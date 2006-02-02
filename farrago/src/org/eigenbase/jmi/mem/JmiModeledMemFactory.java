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

import javax.jmi.reflect.*;
import javax.jmi.model.*;

/**
 * JmiModeledMemFactory augments {@link JmiMemFactory} with information
 * from an underlying metamodel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class JmiModeledMemFactory extends JmiMemFactory
{
    private final JmiModelGraph modelGraph;
    
    public JmiModeledMemFactory(
        JmiModelGraph modelGraph)
    {
        this.modelGraph = modelGraph;
        try {
            definePackages();
            defineClasses();
            defineAssociations();
        } catch (Exception ex) {
            throw Util.newInternal(ex);
        }
    }

    private void definePackages()
        throws ClassNotFoundException
    {
        definePackage(
            modelGraph.getRefRootPackage(),
            getRootPackageImpl().clazz);
    }

    private void definePackage(
        RefPackage refPackageModeled,
        Class iface)
        throws ClassNotFoundException
    {
        defineMetaObject(
            iface,
            refPackageModeled.refMetaObject());
        for (Object pkgObj : refPackageModeled.refAllPackages()) {
            RefPackage childPkg = (RefPackage) pkgObj;
            definePackage(
                childPkg,
                JmiObjUtil.getJavaInterfaceForRefPackage(childPkg));
        }
    }

    private void defineClasses()
        throws ClassNotFoundException
    {
        for (Object vertexObj : modelGraph.vertexSet()) {
            JmiClassVertex vertex = (JmiClassVertex) vertexObj;
            Class ifaceClass =
                JmiObjUtil.getJavaInterfaceForRefClass(vertex.getRefClass());
            Class ifaceObj = 
                JmiObjUtil.getJavaInterfaceForRefObject(vertex.getRefClass());
            defineMetaObject(
                ifaceClass,
                vertex.getMofClass());

            defineMetaObject(
                ifaceObj,
                vertex.getMofClass());
        }
    }

    private void defineAssociations()
        throws ClassNotFoundException
    {
        for (Object edgeObj : modelGraph.getAssocGraph().edgeSet()) {
            JmiAssocEdge edge = (JmiAssocEdge) edgeObj;

            Class ifaceAssoc =
                JmiObjUtil.getJavaInterfaceForRefAssoc(
                    edge.getRefAssoc());
            defineMetaObject(ifaceAssoc, edge.getMofAssoc());
            
            Class sourceInterface = JmiObjUtil.getJavaInterfaceForRefObject(
                ((JmiClassVertex) edge.getSource()).getRefClass());
            String targetAccessorName =
                JmiObjUtil.getAccessorName(edge.getTargetEnd());
            Class [] ec = (Class []) null;
            try {
                sourceInterface.getMethod(targetAccessorName, ec);
            } catch (NoSuchMethodException ex) {
                // No navigation method, so don't create a relationship.
                // (Creating one could actually be harmful in the case
                // where the end name conflicts with another one.)
                continue;
            }
            String targetAttrName = parseGetter(targetAccessorName);
            boolean targetMany =
                (edge.getTargetEnd().getMultiplicity().getUpper() != 1);
                
            Class targetInterface = JmiObjUtil.getJavaInterfaceForRefObject(
                ((JmiClassVertex) edge.getTarget()).getRefClass());
            String sourceAccessorName = 
                JmiObjUtil.getAccessorName(edge.getSourceEnd());
            try {
                targetInterface.getMethod(sourceAccessorName, ec);
            } catch (NoSuchMethodException ex) {
                // No navigation method, so don't create a relationship.
                continue;
            }
            String sourceAttrName = parseGetter(sourceAccessorName);
            boolean sourceMany =
                (edge.getSourceEnd().getMultiplicity().getUpper() != 1);

            boolean isComposite =
                edge.getSourceEnd().getAggregation()
                == AggregationKindEnum.COMPOSITE;
            
            createRelationship(
                sourceInterface,
                targetAttrName,
                targetMany,
                targetInterface,
                sourceAttrName,
                sourceMany,
                isComposite);
        }
    }
}

// End JmiModeledMemFactory.java
