/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 SQLstream, Inc.
// Copyright (C) 2006-2007 LucidEra, Inc.
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

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

import org.jgrapht.graph.*;


/**
 * JmiModeledMemFactory augments {@link JmiMemFactory} with information from an
 * underlying metamodel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class JmiModeledMemFactory
    extends JmiMemFactory
{
    //~ Instance fields --------------------------------------------------------

    private final JmiModelGraph modelGraph;

    //~ Constructors -----------------------------------------------------------

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

    //~ Methods ----------------------------------------------------------------

    private void definePackages()
        throws ClassNotFoundException
    {
        definePackage(
            modelGraph.getRefRootPackage(),
            (Class<? extends RefPackage>) getRootPackageImpl().clazz);
    }

    private void definePackage(
        RefPackage refPackageModeled,
        Class<? extends RefPackage> iface)
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
        for (JmiClassVertex vertex : modelGraph.vertexSet()) {
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

            defineAttributes(vertex);
        }
    }

    private void defineAttributes(JmiClassVertex classVertex)
        throws ClassNotFoundException
    {
        MofClass mofClass = classVertex.getMofClass();
        for (Object obj : mofClass.getContents()) {
            if (!(obj instanceof Attribute)) {
                continue;
            }
            Attribute attr = (Attribute) obj;
            if (!(attr.getScope().equals(ScopeKindEnum.INSTANCE_LEVEL))) {
                continue;
            }
            Classifier attrType = attr.getType();
            if (!(attrType instanceof MofClass)) {
                // primitive type:  no relationship needed
                continue;
            }
            MofClass attrClass = (MofClass) attrType;
            JmiClassVertex attrVertex =
                modelGraph.getVertexForMofClass(attrClass);
            if (attrVertex == null) {
                // some class we don't know about; should probably
                // assert here
                continue;
            }

            // The attribute is actually a class-valued attribute;
            // create a corresponding relationship to tie
            // parent and child objects together.
            Class sourceInterface =
                JmiObjUtil.getJavaInterfaceForRefObject(
                    classVertex.getRefClass());
            String targetAttrName =
                parseGetter(JmiObjUtil.getAccessorName(attr));
            boolean targetMany = (attr.getMultiplicity().getUpper() != 1);
            Class targetInterface =
                JmiObjUtil.getJavaInterfaceForRefObject(
                    attrVertex.getRefClass());
            String sourceAttrName = "ParentOf$" + targetAttrName;
            boolean sourceMany = false;
            boolean isComposite = true;

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

    private void defineAssociations()
        throws ClassNotFoundException
    {
        for (DefaultEdge edgeObj : modelGraph.getAssocGraph().edgeSet()) {
            JmiAssocEdge edge = (JmiAssocEdge) edgeObj;

            Class<? extends RefBaseObject> ifaceAssoc =
                JmiObjUtil.getJavaInterfaceForRefAssoc(
                    edge.getRefAssoc());
            defineMetaObject(
                ifaceAssoc,
                edge.getMofAssoc());

            JmiClassVertex sourceClassVertex =
                modelGraph.getAssocGraph().getEdgeSource(edge);
            Class<? extends RefBaseObject> sourceInterface =
                JmiObjUtil.getJavaInterfaceForRefObject(
                    sourceClassVertex.getRefClass());
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

            JmiClassVertex targetClassVertex =
                modelGraph.getAssocGraph().getEdgeTarget(edge);
            Class targetInterface =
                JmiObjUtil.getJavaInterfaceForRefObject(
                    targetClassVertex.getRefClass());
            String sourceAttrName = null;
            String sourceAccessorName =
                JmiObjUtil.getAccessorName(edge.getSourceEnd());
            try {
                targetInterface.getMethod(sourceAccessorName, ec);

                // Unlike the source end of the association, we'll allow
                // no method here.
                sourceAttrName = parseGetter(sourceAccessorName);
            } catch (NoSuchMethodException ex) {
                // No navigation method, so create a bogus inverse relationship
                sourceAttrName = "ParentOf$" + targetAttrName;
            }
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

    protected JmiModelGraph getModelGraph()
    {
        return modelGraph;
    }
}

// End JmiModeledMemFactory.java
