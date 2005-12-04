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

import org.eigenbase.util.Util;
import org.eigenbase.xom.XMLOutput;

import javax.jmi.reflect.RefBaseObject;
import javax.jmi.reflect.RefClass;
import javax.jmi.reflect.RefPackage;
import java.io.PrintWriter;
import java.lang.reflect.*;
import java.util.*;

import net.sf.farrago.fem.fennel.FemExecutionStreamDef;
import net.sf.farrago.fem.fennel.FemExecStreamDataFlow;

/**
 * Helps create a mock implementation of an MDR metadata factory interface.
 * Mock implementation of metadata factories which implements
 * the MDR interfaces using dynamic proxies. Since this implementation
 * does not persist objects, or even require a repository instance, it is
 * ideal for testing purposes.
 *
 * <p>The name of the class is misleading; it is not itself a metadata factory.
 * MockMetadataFactory uses dynamic proxies (see {@link Proxy}) to
 * generate the necessary interfaces on the fly. Inside every proxy is
 * an instance of {@link ElementImpl}, which stores attributes in a
 * {@link HashMap} and implements the {@link InvocationHandler}
 * interface required by the proxy. There are specialized subtypes of
 * {@link ElementImpl} for packages and classes.
 *
 * <p>Since there is no repository to provide metadata, the factory infers
 * the object model from the Java interfaces:
 * <ul>
 * <li>Each 'get' method is assumed to be an attribute.
 * <li>If the return type extends {@link RefClass}, a factory is created
 *     when the object is initialized.
 * <li>If the return type extends {@link RefPackage}, a sub-package is created
 *     when the object is initialized.
 * <li>If the return type extends {@link Collection}, a collection attribute
 *     is created.
 * <li>All other types are presumed to be regular attributes.
 * </ul>
 */
public abstract class MockMetadataFactory
{
    private int nextId = 0;
    private final RefPackageImpl rootPackageImpl;
    private final Map<String, Relationship> relationshipMap = 
        new HashMap<String, Relationship>();

    public MockMetadataFactory()
    {
        super();
        this.rootPackageImpl = newRootPackage();
        initRelationshipMap();
    }

    /**
     * Registers relationships which we want to be maintained as two-way
     * relationships. This is necessary because we cannot deduce inverse
     * relationships using Java reflection. <p>
     *
     * Derived classes can add override to define additional relationships.
     */
    protected void initRelationshipMap()
    {
        createRelationship(
            FemExecutionStreamDef.class, "InputFlow", true,
            FemExecStreamDataFlow.class, "Consumer", false);
        createRelationship(
            FemExecutionStreamDef.class, "OutputFlow", true,
            FemExecStreamDataFlow.class, "Producer", false);
    }

    /**
     * Creates the right kind of implementation class to implement the
     * given interface.
     */
    protected ElementImpl createImpl(Class clazz, boolean preemptive)
    {
        if (RefClass.class.isAssignableFrom(clazz)) {
            return new RefClassImpl(clazz);
        } else if (RefPackage.class.isAssignableFrom(clazz)) {
            return new RefPackageImpl(clazz);
        } else {
            if (preemptive) {
                return null;
            } else {
                return new ElementImpl(clazz);
            }
        }
    }

    protected abstract RefPackageImpl newRootPackage();

    public RefPackage getRootPackage()
    {
        return (RefPackage) rootPackageImpl.wrap();
    }

    protected static Method[] sortMethods(Method[] methods)
    {
        Method[] sortedMethods = (Method[])methods.clone();
        Arrays.sort(sortedMethods, new Comparator() {
            public int compare(Object o1, Object o2)
            {
                Method m1 = (Method)o1;
                Method m2 = (Method)o2;

                return m1.getName().compareTo(m2.getName());
            }
        });

        return sortedMethods;
    }

    /**
     * Looks up a relationship.
     * 
     * @param fromClass Name class the relationship is from
     * @param fromClass Name of the relationship
     * @return Name of backward relationship, or null if not found
     */ 
    private Relationship lookupRelationship(Class fromClass, String fromName)
    {
        // The clazz parameter is not currently used. We will use this
        // if we ever have more than one relationship with the same name.
        Util.discard(fromClass);
        return relationshipMap.get(fromName);
    }

    /**
     * Creates a relationship definition
     */
    protected void createRelationship(
        Class fromClass,
        String fromName,
        boolean fromMany,
        Class toClass,
        String toName,
        boolean toMany)
    {
        Relationship relationship1 = new Relationship(fromClass, fromName, fromMany);
        Relationship relationship2 = new Relationship(toClass, toName, toMany);
        relationship1.inverse = relationship2;
        relationship2.inverse = relationship1;
        relationshipMap.put(fromName, relationship1);
        relationshipMap.put(toName, relationship2);
    }

    /**
     * Implemented by proxy objects, this interface makes the proxy handler
     * object accessible.
     */
    interface Element {
        ElementImpl impl();
    }

    /**
     * Implementation of a {@link RefBaseObject} via an
     * {@link InvocationHandler} interface.<p>
     *
     * Attributes are held in a {@link TreeMap}. (Not a {@link HashMap},
     * because we want the attributes to be returned in a predictable
     * order.)<p>
     *
     * The current implementation is not very efficient. Every time a method
     * is called, the proxy has to figure out what kind of method this is
     * (relationship accesor, property setter, et cetera) and do the right
     * thing. It would be more efficient, and perhaps cleaner, to create a
     * <dfn>handler map</dfn>: one handler per method. The handler map behaves
     * the same way as a vtable. It is initialiazed once per class, and all
     * that needs to be done when creating an instance is to store the handler
     * map.<p>
     */
    protected class ElementImpl
        extends TreeMap
        implements InvocationHandler
    {
        protected final Class clazz;
        private final int id;
        private final Object proxy;

        ElementImpl(Class clazz)
        {
            this.clazz = clazz;
            this.id = nextId++;
            Method[] methods = clazz.getMethods();
            // Initialize all collections.
            for (int i = 0; i < methods.length; i++) {
                Method method = methods[i];
                String methodName = method.getName();
                Class methodReturn = method.getReturnType();
                if (methodName.startsWith("get") &&
                    method.getParameterTypes().length == 0) {
                    final String attrName = method.getName().substring(3);
                    if (Collection.class.isAssignableFrom(methodReturn)) {
                        initCollection(attrName);
                    }
                }
            }
            proxy = Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[] {clazz, Element.class},
                this);
        }

        private void initCollection(final String collectionName)
        {
            Relationship relationship =
                lookupRelationship(clazz, collectionName);
            if (relationship == null) {
                put(collectionName, new ArrayList());
            } else if (relationship.inverse.many) {
                put(collectionName, new ManyList(this, relationship));
            } else {
                put(collectionName, new OneWayList(this, relationship));
            }
        }

        public Object invoke(
            Object proxy, Method method, Object[] args) throws Throwable
        {
            String methodName = method.getName();
            if (method.getDeclaringClass() == Object.class) {
                // Intercept methods on Object. Otherwise we loop.
                if (methodName.equals("toString")) {
                    return clazz + ":" + System.identityHashCode(proxy);
                } else if (methodName.equals("hashCode")) {
                    return new Integer(System.identityHashCode(proxy));
                } else if (methodName.equals("equals")) {
                    return Boolean.valueOf(proxy == args[0]);
                } else {
                    throw new UnsupportedOperationException();
                }
            } else if (method.getDeclaringClass() == Comparable.class) {
                assert methodName.equals("compareTo");
                assert args == null;
                RefBaseObject that = (RefBaseObject) args[0];
                String thisMofId = this.proxyRefMofId();
                String thatMofId = that.refMofId();
                return new Integer(thisMofId.compareTo(thatMofId));
            } else if (methodName.equals("refMofId")) {
                assert method.getDeclaringClass() == RefBaseObject.class;
                assert args == null;
                return proxyRefMofId();
            } else if (methodName.equals("impl")) {
                assert method.getDeclaringClass() == Element.class;
                assert args == null;
                return proxyImpl();
            } else if (methodName.startsWith("get")) {
                assert args == null;
                return proxyGet(methodName.substring(3), method, args);
            } else if (methodName.startsWith("set")) {
                return proxySet(methodName.substring(3), method, args);
            } else if (methodName.startsWith("create")) {
                return proxyCreate(methodName.substring(6), args,
                    method.getReturnType());
            } else {
                throw new UnsupportedOperationException(method.toString());
            }
        }

        protected String proxyRefMofId()
        {
            return "x:" + id;
        }

        protected Object proxyCreate(
            String attrName,
            Object[] args,
            Class createClass)
        {
            throw new UnsupportedOperationException();
        }

        protected Object proxySet(
            String attrName, Method method, Object[] args)
        {
            Class attrClass = method.getReturnType();
            assert args.length == 1;
            final Object o = args[0];
            if (RefBaseObject.class.isAssignableFrom(attrClass)) {
                final Relationship relationship = lookupRelationship(clazz, attrName);
                if (relationship != null) {
                    // Add the object at the other end.
                    final ElementImpl elementImpl = ((Element) o).impl();
                    OneWayList inverseCollection =
                        (OneWayList)
                        elementImpl.get(relationship.inverse.name);
                    inverseCollection.add(proxy);
                }
            }
            return put(attrName, o);
        }

        protected Object proxyGet(
            String attrName, Method method, Object[] args)
        {
            Class attrClass = method.getReturnType();
            assert args == null;
            Object o = get(attrName);
            if (o == null && attrClass.isPrimitive()) {
                // Primitive values cannot be null. So, provide a value.
                if (attrClass == int.class) {
                    return new Integer(0);
                }
            }
            return o;
        }

        protected ElementImpl proxyImpl()
        {
            return this;
        }

        /**
         * Returns this object wrapped in a proxy. The proxy will implement the
         * interface specified in the {@link #clazz} member.
         */
        protected Object wrap()
        {
            return proxy;
        }
    }

    /**
     * Specialized handler for implementing a {@link RefClass} via a dynamic
     * proxy.
     */
    protected class RefClassImpl extends ElementImpl
    {
        RefClassImpl(Class clazz)
        {
            super(clazz);
        }

        protected Object proxyCreate(
            String attrName, Object[] args, Class createClass)
        {
            assert args == null;
            return createImpl(createClass, false).wrap();
        }
    }

    /**
     * Specialized handler for implementing a {@link RefPackage} via a dynamic
     * proxy.
     */
    protected class RefPackageImpl extends ElementImpl
    {
        public RefPackageImpl(Class clazz)
        {
            super(clazz);
            // For each method which returns a class, create an attribute.
            Method[] methods = sortMethods(clazz.getMethods());
            for (int i = 0; i < methods.length; i++) {
                Method method = methods[i];
                String methodName = method.getName();
                if (methodName.startsWith("get") &&
                    method.getParameterTypes().length == 0) {
                    String attrName = methodName.substring(3);
                    Class attributeClass = method.getReturnType();
                    // Attribute is class.
                    ElementImpl el = createImpl(attributeClass, true);
                    if (el != null) {
                        put(attrName, el.wrap());
                    }
                }
            }
        }

    }

    /**
     * Definition of a relationship.
     */ 
    static class Relationship
    {
        final Class clazz;
        final String name;
        final boolean many;
        Relationship inverse;

        public Relationship(Class clazz, String name, boolean many)
        {
            super();    
            this.clazz = clazz;
            this.name = name;
            this.many = many;
        }
    }
    
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
                attrNames, attrValues,
                collectionNames, collectionValues,
                refNames, refValues);
            visit(
                o,
                attrNames, attrValues,
                collectionNames, collectionValues,
                refNames, refValues);
        }

        /**
         * From a data object, builds lists of attributes, collections,
         * and references.
         */
        protected void extractProperties(
            Object o,
            List attrNames, List attrValues,
            List collectionNames, List collectionValues,
            List refNames, List refValues)
        {
            Class clazz = o.getClass();
            Method[] methods = sortMethods(clazz.getMethods());
            for (int i = 0; i < methods.length; i++) {
                Method method = methods[i];
                String methodName = method.getName();
                if (methodName.startsWith("get") &&
                    method.getParameterTypes().length == 0 &&
                    method.getDeclaringClass() != Object.class) {
                    String attrName =
                        methodName.substring(3, 4).toLowerCase() +
                        methodName.substring(4);
                    Class attrClass = method.getReturnType();
                    Object attrValue;
                    try {
                        attrValue = method.invoke(o, (Object[]) null);
                    } catch (IllegalAccessException e) {
                        throw Util.newInternal(e);
                    } catch (InvocationTargetException e) {
                        throw Util.newInternal(e);
                    }
                    if (Collection.class.isAssignableFrom(attrClass)) {
                        collectionNames.add(attrName);
                        collectionValues.add((Collection) attrValue);
                    } else if (RefBaseObject.class.isAssignableFrom(attrClass)) {
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
    public static class JmiPrinter extends JmiVisitor
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
            xmlOutput.attribute(attrName, String.valueOf(attrValue));
        }

        protected void onElement(Object o)
        {
        }

        protected String getTagName(Object o)
        {
            String className = o.getClass().getInterfaces()[0].getName();
            int dot = className.lastIndexOf('.');
            String tagName = (dot >= 0) ?
                className.substring(dot + 1) :
                className;
            return tagName;
        }
    }

    /**
     * List which holds instances of a many-to-one relationship.
     */
    private static class OneWayList extends ArrayList
    {
        private final ElementImpl element;
        private final Relationship relationship;

        OneWayList(
            ElementImpl element,
            Relationship relationship)
        {
            super();
            assert element != null;
            assert relationship != null;
            this.element = element;
            this.relationship = relationship;
            assert relationship.many && !relationship.inverse.many;
        }

        public boolean add(Object o)
        {
            // Set the pointer at the other end. (Do not use the 'set' method,
            // because this would try to modify this list, and that would
            // loop.)
            final ElementImpl elementImpl = ((Element) o).impl();
            elementImpl.put(relationship.inverse.name, element.proxy);
            return super.add(o);
        }
    }

    /**
     * List which holds instances of a bi-directional relationship.<p/>
     * 
     * When an instance of the relationship is created, by calling 
     * {@link #add(Object)} to the collection at one end, this collection
     * automatically finds the corresponding collection at the other end
     * and calls {@link #addInternal(ElementImpl)}. 
     */ 
    private static class ManyList extends ArrayList
    {
        private final ElementImpl element;
        private final Relationship relationship;

        ManyList(ElementImpl element, Relationship relationship)
        {
            super();
            assert element != null;
            assert relationship != null;
            assert relationship.many && relationship.inverse.many;
            this.element = element;
            this.relationship = relationship;
        }

        public boolean add(Object o)
        {
            // Add the object at the other end.
            final ElementImpl elementImpl = ((Element) o).impl();
            ManyList inverseCollection =
                (ManyList) elementImpl.get(relationship.inverse.name);
            inverseCollection.addInternal(element);
            return super.add(o);
        }

        protected void addInternal(ElementImpl source)
        {
            // Add object to the collection. Do not call the add(Object)
            // method, otherwise addition would be cyclic.
            super.add(source);
        }
    }
}

// End MockMetadataFactory.java
