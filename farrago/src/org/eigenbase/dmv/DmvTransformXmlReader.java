/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 SQLstream, Inc.
// Copyright (C) 2007-2007 LucidEra, Inc.
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
package org.eigenbase.dmv;

import java.io.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import javax.xml.parsers.*;

import org.eigenbase.jmi.*;

import org.w3c.dom.*;


/**
 * DmvTransformXmlReader reads a definition of a DMV transformation from an XML
 * file and applies it to a {@link JmiDependencyMappedTransform}.
 *
 * <p>The XML file must be valid according to <code>
 * farrago/examples/dmv/DmvTransformationRuleSet.dtd</code>.
 *
 * @author John Sichi
 * @version $Id$
 */
public class DmvTransformXmlReader
{
    //~ Static fields/initializers ---------------------------------------------

    private static final String ELEMENT_SET_ALL_BY_AGGREGATION =
        "SetAllByAggregation";
    private static final String ELEMENT_SET_BY_REF_ASSOC = "SetByAssoc";
    private static final String ATTRIBUTE_REQUESTED_KIND = "requestedKind";
    private static final String ATTRIBUTE_MAPPING = "mapping";
    private static final String ATTRIBUTE_ASSOC = "assoc";
    private static final String ATTRIBUTE_SOURCE_CLASS = "sourceClass";
    private static final String ATTRIBUTE_TARGET_CLASS = "targetClass";

    //~ Instance fields --------------------------------------------------------

    private final JmiModelGraph modelGraph;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new DmvTransformXmlReader.
     *
     * @param modelGraph model graph to use for metamodel lookup
     */
    public DmvTransformXmlReader(JmiModelGraph modelGraph)
    {
        this.modelGraph = modelGraph;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Converts the XML representation of a DMV transformation into a {@link
     * JmiDependencyMappedTransform}.
     *
     * @param filename name of XML file to read
     * @param transform transform to initialize
     */
    public void readTransformationRules(
        String filename,
        JmiDependencyMappedTransform transform)
        throws Exception
    {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = dbf.newDocumentBuilder();
        FileInputStream fis = new FileInputStream(filename);
        try {
            Document doc = docBuilder.parse(new FileInputStream(filename));
            Element root = doc.getDocumentElement();
            NodeList nodeList = root.getChildNodes();
            for (int i = 0; i < nodeList.getLength(); ++i) {
                Node node = nodeList.item(i);
                String nodeName = node.getNodeName();
                if (nodeName.equals(ELEMENT_SET_ALL_BY_AGGREGATION)) {
                    setAllByAggregation((Element) node,
                        transform);
                } else if (nodeName.equals(ELEMENT_SET_BY_REF_ASSOC)) {
                    setByRefAssoc((Element) node,
                        transform);
                }
            }
        } finally {
            fis.close();
        }
    }

    private void setAllByAggregation(
        Element rule,
        JmiDependencyMappedTransform transform)
    {
        String kindName = rule.getAttribute(ATTRIBUTE_REQUESTED_KIND);
        String mappingName = rule.getAttribute(ATTRIBUTE_MAPPING);
        transform.setAllByAggregation(
            AggregationKindEnum.forName(kindName),
            Enum.valueOf(JmiAssocMapping.class, mappingName));
    }

    private void setByRefAssoc(
        Element rule,
        JmiDependencyMappedTransform transform)
    {
        String assocName = rule.getAttribute(ATTRIBUTE_ASSOC);
        JmiAssocEdge assocEdge = modelGraph.getEdgeForAssocName(assocName);
        if (assocEdge == null) {
            throw new IllegalArgumentException(assocName);
        }
        String mappingName = rule.getAttribute(ATTRIBUTE_MAPPING);
        String sourceClassName = rule.getAttribute(ATTRIBUTE_SOURCE_CLASS);
        String targetClassName = rule.getAttribute(ATTRIBUTE_TARGET_CLASS);
        JmiAssocMapping mapping =
            Enum.valueOf(JmiAssocMapping.class, mappingName);
        if ((sourceClassName.equals("")) && (targetClassName.equals(""))) {
            transform.setByRefAssoc(
                assocEdge.getRefAssoc(),
                mapping);
            return;
        }
        RefClass refSourceClass = getRefClass(sourceClassName);
        RefClass refTargetClass = getRefClass(targetClassName);
        transform.setByRefAssocRefined(
            assocEdge.getRefAssoc(),
            mapping,
            refSourceClass,
            refTargetClass);
    }

    private RefClass getRefClass(
        String className)
    {
        if (className.equals("")) {
            return null;
        }
        JmiClassVertex classVertex =
            modelGraph.getVertexForClassName(className);
        if (classVertex == null) {
            throw new IllegalArgumentException(className);
        }
        return classVertex.getRefClass();
    }
}

// End DmvTransformXmlReader.java
