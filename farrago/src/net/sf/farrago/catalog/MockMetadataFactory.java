/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.catalog;

import java.io.*;

import java.lang.reflect.*;

import java.util.*;

import javax.jmi.reflect.*;

import net.sf.farrago.fem.fennel.*;

import org.eigenbase.jmi.mem.*;
import org.eigenbase.util.*;
import org.eigenbase.xom.*;


/**
 * Helps create a mock implementation of an MDR metadata factory interface. Mock
 * implementation of metadata factories which implements the MDR interfaces
 * using dynamic proxies. Since this implementation does not persist objects, or
 * even require a repository instance, it is ideal for testing purposes.
 *
 * <p>The name of the class is misleading; it is not itself a metadata factory.
 * MockMetadataFactory uses dynamic proxies (see {@link Proxy}) to generate the
 * necessary interfaces on the fly. Inside every proxy is an instance of {@link
 * ElementImpl}, which stores attributes in a {@link HashMap} and implements the
 * {@link InvocationHandler} interface required by the proxy. There are
 * specialized subtypes of {@link ElementImpl} for packages and classes.
 *
 * <p>Since there is no repository to provide metadata, the factory infers the
 * object model from the Java interfaces:
 *
 * <ul>
 * <li>Each 'get' method is assumed to be an attribute.
 * <li>If the return type extends {@link RefClass}, a factory is created when
 * the object is initialized.
 * <li>If the return type extends {@link RefPackage}, a sub-package is created
 * when the object is initialized.
 * <li>If the return type extends {@link Collection}, a collection attribute is
 * created.
 * <li>All other types are presumed to be regular attributes.
 * </ul>
 */
public abstract class MockMetadataFactory
    extends JmiMemFactory
{

    //~ Constructors -----------------------------------------------------------

    public MockMetadataFactory()
    {
        super();
        initRelationshipMap();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Registers relationships which we want to be maintained as two-way
     * relationships. This is necessary because we cannot deduce inverse
     * relationships using Java reflection.
     *
     * <p>Derived classes can add override to define additional relationships.
     */
    protected void initRelationshipMap()
    {
        createRelationship(
            FemExecutionStreamDef.class,
            "InputFlow",
            true,
            FemExecStreamDataFlow.class,
            "Consumer",
            false);
        createRelationship(
            FemExecutionStreamDef.class,
            "OutputFlow",
            true,
            FemExecStreamDataFlow.class,
            "Producer",
            false);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Abstract base class for an iterator over a JMI object and its children.
     *
     * <p>The current implementation isn't very elegant. It makes a lot of
     * assumptions about how the JMI tree is represented. We should adopt a
     * 'real' visitor pattern -- with an 'accept' method on each handler object
     * -- if this class is ever made public.
     */
    static abstract class JmiVisitor
    {
        protected void accept(Object o)
        {
            List attrNames = new ArrayList();
            List attrValues = new ArrayList();
            List collectionNames = new ArrayList();
            List collectionValues = new ArrayList();
            List refNames = new ArrayList();
            List refValues = new ArrayList();
            extractProperties(
                o,
                attrNames,
                attrValues,
                collectionNames,
                collectionValues,
                refNames,
                refValues);
            visit(
                o,
                attrNames,
                attrValues,
                collectionNames,
                collectionValues,
                refNames,
                refValues);
        }

        /**
         * From a data object, builds lists of attributes, collections, and
         * references.
         */
        protected void extractProperties(
            Object o,
            List attrNames,
            List attrValues,
            List collectionNames,
            List collectionValues,
            List refNames,
            List refValues)
        {
            Class clazz = o.getClass();
            Method [] methods = sortMethods(clazz.getMethods());
            for (int i = 0; i < methods.length; i++) {
                Method method = methods[i];
                String methodName = method.getName();
                if (methodName.startsWith("get")
                    && (method.getParameterTypes().length == 0)
                    && (method.getDeclaringClass() != Object.class)) {
                    String attrName =
                        methodName.substring(3, 4).toLowerCase()
                        + methodName.substring(4);
                    Class attrClass = method.getReturnType();
                    Object attrValue;
                    try {
                        attrValue = method.invoke(o, (Object []) null);
                    } catch (IllegalAccessException e) {
                        throw Util.newInternal(e);
                    } catch (InvocationTargetException e) {
                        throw Util.newInternal(e);
                    }
                    if (Collection.class.isAssignableFrom(attrClass)) {
                        collectionNames.add(attrName);
                        collectionValues.add((Collection) attrValue);
                    } else if (RefBaseObject.class.isAssignableFrom(
                            attrClass)) {
                        refNames.add(attrName);
                        refValues.add((RefBaseObject) attrValue);
                    } else {
                        attrNames.add(attrName);
                        attrValues.add(attrValue);
                    }
                }
            }
        }

        protected abstract void visit(
            Object o,
            List attrNames,
            List attrValues,
            List collectionNames,
            List collectionValues,
            List refNames,
            List refValues);
    }

    /**
     * Formats a JMI object as XML.
     */
    public static class JmiPrinter
        extends JmiVisitor
    {
        // ~ Data members
        protected final XMLOutput xmlOutput;

        protected JmiPrinter(PrintWriter pw)
        {
            xmlOutput = new XMLOutput(pw);
            xmlOutput.setGlob(true);
        }

        protected void visit(
            Object o,
            List attrNames,
            List attrValues,
            List collectionNames,
            List collectionValues,
            List refNames,
            List refValues)
        {
            String tagName = getTagName(o);
            xmlOutput.beginBeginTag(tagName);
            onElement(o);
            for (int i = 0; i < attrNames.size(); i++) {
                String attrName = (String) attrNames.get(i);
                Object attrValue = attrValues.get(i);
                onAttribute(attrName, attrValue);
            }
            xmlOutput.endBeginTag(tagName);
            for (int i = 0; i < refNames.size(); i++) {
                String refName = (String) refNames.get(i);
                RefBaseObject ref = (RefBaseObject) refValues.get(i);
                onRef(refName, ref);
                continue;
            }
            for (int i = 0; i < collectionNames.size(); i++) {
                String collectionName = (String) collectionNames.get(i);
                List list = (List) collectionValues.get(i);
                onCollection(collectionName, list);
            }
            xmlOutput.endTag(tagName);
        }

        protected void onCollection(String collectionName, List list)
        {
            xmlOutput.beginTag(collectionName, null);
            for (int j = 0; j < list.size(); j++) {
                RefBaseObject ref = (RefBaseObject) list.get(j);
                accept(ref);
            }
            xmlOutput.endTag(collectionName);
        }

        protected void onRef(String refName, RefBaseObject ref)
        {
            xmlOutput.beginTag(refName, null);
            accept(ref);
            xmlOutput.endTag(refName);
        }

        protected void onAttribute(String attrName, Object attrValue)
        {
            xmlOutput.attribute(
                attrName,
                String.valueOf(attrValue));
        }

        protected void onElement(Object o)
        {
        }

        protected String getTagName(Object o)
        {
            String className = o.getClass().getInterfaces()[0].getName();
            int dot = className.lastIndexOf('.');
            String tagName =
                (dot >= 0) ? className.substring(dot + 1) : className;
            return tagName;
        }
    }
}

// End MockMetadataFactory.java
