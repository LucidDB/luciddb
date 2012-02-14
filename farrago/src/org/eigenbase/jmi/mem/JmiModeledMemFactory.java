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
            if (vertex.getRefClass() == null) {
                continue;
            }
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

            if (edge.getRefAssoc() == null) {
                continue;
            }

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
