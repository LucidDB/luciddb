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

import org.eigenbase.util.*;
import org.eigenbase.jmi.*;

import javax.jmi.reflect.*;
import javax.jmi.model.*;
import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * JmiMemFactory creates objects for use in an in-memory repository
 * implementation.
 *
 * @author Julian Hyde
 * @author John V. Sichi
 *
 * @version $Id$
 */
public abstract class JmiMemFactory
{
    private final AtomicInteger nextId;
    private final RefPackageImpl rootPackageImpl;
    private final Map<String, Relationship> relationshipMap;
    private final Map<Class, RefObject> metaMap;
    private final Map<RefObject, RefPackage> pluginPackageMap;
    private final Map<Class, RefClass> classMap;

    public JmiMemFactory()
    {
        nextId = new AtomicInteger(0);
        relationshipMap = new HashMap<String, Relationship>();
        metaMap = new HashMap<Class, RefObject>();
        pluginPackageMap = new HashMap<RefObject, RefPackage>();
        classMap = new HashMap<Class, RefClass>();
        
        this.rootPackageImpl = newRootPackage();
    }
    
    public RefPackage getRootPackage()
    {
        return (RefPackage) rootPackageImpl.wrap();
    }

    /**
     * Associates the MOFID of a persistent object with an in-memory
     * object.  This is useful when the in-memory object is being
     * manipulated as a shadow of the persistent object.
     *
     * @param obj in-memory object
     *
     * @param persistentMofId MOFID of persistent object to set
     */
    public void setPersistentMofId(RefBaseObject obj, String persistentMofId)
    {
        ElementImpl impl = ((Element) obj).impl();
        impl.persistentMofId = persistentMofId;
    }

    /**
     * Gets the MOFID of a persistent object associated with an in-memory
     * object.
     *
     * @param obj in-memory object
     *
     * @return MOFID of persistent object, or null if none set
     */
    public String getPersistentMofId(RefBaseObject obj)
    {
        ElementImpl impl = ((Element) obj).impl();
        return impl.persistentMofId;
    }

    /**
     * Creates a new package explicitly (rather than reflectively), locating it
     * as a child of the root package.  This is required for plugin
     * sub-packages, which don't have corresponding accessor methods on the
     * root package.
     *
     * @param ifacePackage interface corresponding to RefPackage
     *
     * @return new RefPackage
     */
    public RefPackage newRefPackage(Class ifacePackage)
    {
        ElementImpl impl = createImpl(ifacePackage, true);
        RefPackage refPackage = (RefPackage) impl.wrap();
        RefObject refMetaObj = null;
        if (metaMap != null) {
            refMetaObj = metaMap.get(ifacePackage);
        }
        if (refMetaObj != null) {
            pluginPackageMap.put(refMetaObj, refPackage);
        }
        return refPackage;
    }

    /**
     * Notifies subclasses that a new object has been created with the
     * given MOFID.
     *
     * @param refObj new object
     *
     * @param mofId MOFID assigned
     */
    protected void mapMofId(RefBaseObject refObj, String mofId)
    {
    }

    protected abstract RefPackageImpl newRootPackage();

    protected RefPackageImpl getRootPackageImpl()
    {
        return rootPackageImpl;
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
        } else if (RefAssociation.class.isAssignableFrom(clazz)) {
            return new RefAssociationImpl(clazz);
        } else {
            if (preemptive) {
                return null;
            } else {
                return new ElementImpl(clazz);
            }
        }
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
        Relationship relationship = 
            relationshipMap.get(fromClass.getName() + ":" + fromName);
        if (relationship != null) {
            return relationship;
        }

        // Try inherited interfaces, recursively.
        Class [] interfaces = fromClass.getInterfaces();
        for (Class iface : interfaces) {
            relationship = lookupRelationship(iface, fromName);
            if (relationship != null) {
                return relationship;
            }
        }
        
        return null;
    }

    /**
     * Creates a relationship definition
     */
    public void createRelationship(
        Class fromClass,
        String fromName,
        boolean fromMany,
        Class toClass,
        String toName,
        boolean toMany)
    {
        createRelationship(
            fromClass, fromName, fromMany, toClass, toName, toMany,
            false);
    }

    /**
     * Defines a meta-object.
     *
     * @param iface Java interface to associate with meta-object
     *
     * @param metaObject meta-object to return for refMetaObject()
     * from instances of iface
     */
    public void defineMetaObject(Class iface, RefObject metaObject)
    {
        metaMap.put(iface, metaObject);
    }

    /**
     * Creates a relationship definition
     */
    public void createRelationship(
        Class fromClass,
        String fromName,
        boolean fromMany,
        Class toClass,
        String toName,
        boolean toMany,
        boolean composite)
    {
        Relationship relationship1 = new Relationship(
            fromClass, fromName, fromMany);
        Relationship relationship2 = new Relationship(
            toClass, toName, toMany);
        relationship1.inverse = relationship2;
        relationship2.inverse = relationship1;
        if (composite) {
            relationship1.compositeParent = true;
            relationship2.compositeChild = true;
        }
        relationshipMap.put(
            fromClass.getName() + ":" + fromName,
            relationship1);
        relationshipMap.put(
            toClass.getName() + ":" + toName,
            relationship2);
    }

    /**
     * Implemented by proxy objects, this interface makes the proxy handler
     * object accessible.
     */
    interface Element {
        ElementImpl impl();
    }

    protected String parseGetter(String methodName)
    {
        if (methodName.startsWith("get")) {
            return methodName.substring(3);
        } else if (methodName.startsWith("is")) {
            return methodName.substring(2);
        } else {
            return null;
        }
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
        extends TreeMap<String, Object>
        implements InvocationHandler
    {
        protected final Class clazz;
        private final int id;
        private final Object proxy;
        String persistentMofId;

        ElementImpl(Class clazz)
        {
            this.clazz = clazz;
            this.id = nextId.getAndIncrement();
            Method[] methods = clazz.getMethods();
            // Initialize all collections.
            for (int i = 0; i < methods.length; i++) {
                Method method = methods[i];
                String methodName = method.getName();
                Class methodReturn = method.getReturnType();
                String attrName = parseGetter(methodName);
                // TODO jvs 31-Jan-2006: Handle 1-to-1 associations like
                // FarragoConfiguresFennel.  There's no collection
                // involved, but a bidirectional link is still required.
                if ((attrName != null) &&
                    method.getParameterTypes().length == 0) {
                    if (Collection.class.isAssignableFrom(methodReturn)) {
                        initCollection(attrName);
                    }
                }
            }
            proxy = Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[] {clazz, Element.class},
                this);
            mapMofId((RefBaseObject) proxy, proxyRefMofId());
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
            } else if (parseGetter(methodName) != null) {
                assert args == null;
                return proxyGet(parseGetter(methodName), method, args);
            } else if (methodName.startsWith("set")) {
                return proxySet(methodName.substring(3), method, args);
            } else if (methodName.startsWith("create")) {
                return proxyCreate(methodName.substring(6), args,
                    method.getReturnType());
            } else if (methodName.equals("refClass") && (args == null)) {
                return classMap.get(clazz);
            } else if (methodName.equals("refImmediateComposite")) {
                return proxyImmediateComposite();
            } else if (methodName.equals("refOutermostPackage")) {
                return rootPackageImpl.wrap();
            } else if (methodName.equals("refAllPackages")) {
                return proxyRefAllPackages();
            } else if (methodName.equals("refAllClasses")) {
                return filterChildren(RefClass.class);
            } else if (methodName.equals("refAllAssociations")) {
                return filterChildren(RefAssociation.class);
            } else if (methodName.equals("refMetaObject")) {
                Object obj = metaMap.get(clazz);
                assert(obj != null) : clazz;
                return obj;
            } else if (methodName.equals("refGetEnum")) {
                return proxyRefGetEnum(args[0], (String) args[1]);
            } else if (methodName.equals("refGetValue")) {
                return proxyRefByMoniker(args[0]);
            } else if (methodName.equals("refSetValue")) {
                return proxyRefSetValue(args[0], args[1]);
            } else if (methodName.equals("refPackage")) {
                return proxyRefPackage(args[0]);
            } else if (methodName.equals("refClass")) {
                return proxyRefByMoniker(args[0]);
            } else if (methodName.equals("refAssociation")) {
                return proxyRefByMoniker(args[0]);
            } else if (methodName.equals("refDelete")) {
                // REVIEW jvs 9-Mar-2006: this is to allow code to be reusable
                // across persistent and mem repositories, but the behavior
                // will be different!
                return null;
            } else if (methodName.equals("refAllLinks")) {
                // REVIEW jvs 30-Jan-2006:  To implement this, we
                // would have to keep track of extents, which we don't
                // want to do.  Instead of failing, return empty set
                // so that XMI export can work (assuming the model
                // does not contain an association without a corresponding
                // reference on at least one side).
                return Collections.EMPTY_SET;
            } else if (methodName.equals("refLinkExists")) {
                return proxyRefLinkExists(
                    (RefObject) args[0],
                    (RefObject) args[1]);
            } else if (methodName.equals("refAddLink")) {
                return proxyRefAddLink(
                    (RefObject) args[0],
                    (RefObject) args[1]);
            } else if (methodName.equals("refRemoveLink")) {
                return proxyRefRemoveLink(
                    (RefObject) args[0],
                    (RefObject) args[1]);
            } else if (methodName.equals("refCreateInstance")) {
                return proxyRefCreateInstance((List) args[0]);
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
            String attrName, Method method, Object [] args)
        {
            assert args.length == 1;
            Class attrClass = method.getParameterTypes()[0];
            final Object o = args[0];
            return proxySet(
                attrName,
                o,
                RefBaseObject.class.isAssignableFrom(attrClass));
        }

        protected Object proxySet(
            String attrName, Object value, boolean needRelationshipCheck)
        {
            if (needRelationshipCheck) {
                final Relationship relationship = lookupRelationship(
                    clazz, attrName);
                if (relationship != null) {
                    // Add the object at the other end.
                    final ElementImpl elementImpl =
                        ((Element) value).impl();
                    if (relationship.inverse.many) {
                        OneWayList inverseCollection =
                            (OneWayList)
                            elementImpl.get(relationship.inverse.name);
                        inverseCollection.add(proxy);
                    } else {
                        elementImpl.put(
                            relationship.inverse.name,
                            proxy);
                    }
                }
            }
            return put(attrName, value);
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

        protected Object proxyImmediateComposite()
        {
            for (String attrName : keySet()) {
                Relationship r = lookupRelationship(clazz, attrName);
                if (r == null) {
                    continue;
                }
                if (!r.compositeChild) {
                    continue;
                }
                Object obj = get(attrName);
                if (obj != null) {
                    return obj;
                }
            }
            return null;
        }

        protected Object proxyRefPackage(Object moniker)
        {
            RefPackage refPackage = pluginPackageMap.get(moniker);
            if (refPackage != null) {
                return refPackage;
            }
            return proxyRefByMoniker(moniker);
        }

        protected Collection proxyRefAllPackages()
        {
            Collection<RefPackage> children = filterChildren(RefPackage.class);
            if (this == rootPackageImpl) {
                Collection<RefPackage> list = new ArrayList<RefPackage>();
                list.addAll(children);
                list.addAll(pluginPackageMap.values());
                return list;
            } else {
                return children;
            }
        }

        protected Object proxyRefByMoniker(Object moniker)
        {
            String accessorName = getAccessorName(moniker);
            Object result = get(parseGetter(accessorName));
            return result;
        }

        protected Object proxyRefSetValue(Object moniker, Object value)
        {
            String accessorName = getAccessorName(moniker);
            Object result = proxySet(
                parseGetter(accessorName),
                value,
                true);
            return result;
        }

        private String getAccessorName(Object moniker)
        {
            // TODO jvs 30-Jan-2006:  handle case where
            // moniker instanceof String
            ModelElement modelElement = (ModelElement) moniker;
            return JmiObjUtil.getAccessorName(modelElement);
        }

        protected Object proxyRefGetEnum(Object moniker, String name)
            throws Throwable
        {
            // TODO jvs 30-Jan-2006:  handle case where
            // moniker instanceof String
            ModelElement modelElement = (ModelElement) moniker;
            String packageName = clazz.getPackage().getName();
            String enumClassName =
                packageName + "." + modelElement.getName() + "Enum";
            Class enumClass = Class.forName(enumClassName);
            Field field = enumClass.getField(JmiObjUtil.getEnumFieldName(name));
            return field.get(null);
        }

        protected Boolean proxyRefLinkExists(
            RefObject firstEnd,
            RefObject secondEnd)
        {
            ResolvedReference rr = resolveReference(firstEnd, secondEnd);
            Object obj = rr.referencingEnd.refGetValue(rr.ref);
            if (rr.multiValued) {
                Collection c = (Collection) obj;
                return c.contains(rr.referencedEnd);
            } else {
                return obj == rr.referencedEnd;
            }
        }

        protected Boolean proxyRefAddLink(
            RefObject firstEnd,
            RefObject secondEnd)
        {
            Association assoc = (Association) metaMap.get(clazz);
            if (proxyRefLinkExists(firstEnd, secondEnd)) {
                return Boolean.FALSE;
            }
            ResolvedReference rr = resolveReference(firstEnd, secondEnd);
            if (rr.multiValued) {
                Collection c = (Collection)
                    rr.referencingEnd.refGetValue(rr.ref);
                c.add(rr.referencedEnd);
            } else {
                rr.referencingEnd.refSetValue(rr.ref, rr.referencedEnd);
            }
            return Boolean.TRUE;
        }

        protected Boolean proxyRefRemoveLink(
            RefObject firstEnd,
            RefObject secondEnd)
        {
            if (!proxyRefLinkExists(firstEnd, secondEnd)) {
                return Boolean.FALSE;
            }
            ResolvedReference rr = resolveReference(firstEnd, secondEnd);
            if (rr.multiValued) {
                Collection c = (Collection)
                    rr.referencingEnd.refGetValue(rr.ref);
                c.remove(rr.referencedEnd);
            } else {
                rr.referencingEnd.refSetValue(rr.ref, null);
            }
            return Boolean.TRUE;
        }

        private class ResolvedReference
        {
            Reference ref;
            RefObject referencingEnd;
            RefObject referencedEnd;
            boolean multiValued;
        }
        
        private ResolvedReference resolveReference(
            RefObject firstEnd,
            RefObject secondEnd)
        {
            Association assoc = (Association) metaMap.get(clazz);
            AssociationEnd assocFirstEnd =
                (AssociationEnd) (assoc.getContents().get(0));
            AssociationEnd assocSecondEnd =
                (AssociationEnd) (assoc.getContents().get(1));

            ResolvedReference rr = new ResolvedReference();

            // First time through we try for the "one" side instead of
            // the "many" side, because that's a little bit more efficient
            // in some uses.
            for (;;) {

                // Try secondEnd references firstEnd.
                rr.referencingEnd = secondEnd;
                rr.referencedEnd = firstEnd;
                if (attemptReferenceResolution(rr, assocFirstEnd)) {
                    return rr;
                }

                // Try firstEnd references secondEnd.
                rr.referencingEnd = firstEnd;
                rr.referencedEnd = secondEnd;
                if (attemptReferenceResolution(rr, assocSecondEnd)) {
                    return rr;
                }

                if (!rr.multiValued) {
                    // Try multi-valued next time.
                    rr.multiValued = true;
                } else {
                    // Already tried both single-valued and multi-valued;
                    // give up.
                    break;
                }
            }
            
            throw Util.newInternal(
                "unresolved reference for association "
                + clazz + " with firstEnd = " + firstEnd
                + " and secondEnd = " + secondEnd);
        }

        private boolean attemptReferenceResolution(
            ResolvedReference rr,
            AssociationEnd referencedEnd)
        {
            for (Reference featureRef :
                JmiObjUtil.getFeatures(
                    rr.referencingEnd.refClass(),
                    Reference.class,
                    rr.multiValued)) {
                if (featureRef.getReferencedEnd() == referencedEnd) {
                    rr.ref = featureRef;
                    return true;
                }
            }
            return false;
        }
        
        protected Object proxyRefCreateInstance(List args)
        {
            Class createClass = null;
            Method[] methods = clazz.getMethods();
            for (int i = 0; i < methods.length; ++i) {
                if (!methods[i].getName().startsWith("create")) {
                    continue;
                }
                createClass = methods[i].getReturnType();
                break;
            }
            assert(createClass != null);
            ElementImpl impl = createImpl(createClass, false);

            Iterator featureIter = JmiObjUtil.getFeatures(
                (RefClass) wrap(),
                Attribute.class,
                true).iterator();

            for (Object arg : args) {
                StructuralFeature feature = (StructuralFeature)
                    featureIter.next();
                impl.proxySet(
                    parseGetter(JmiObjUtil.getAccessorName(feature)),
                    arg,
                    true);
            }
            return impl.wrap();
        }

        protected <T> Collection<T> filterChildren(Class<T> iface)
        {
            List<T> list = new ArrayList<T>();
            for (Object obj : values()) {
                if (iface.isInstance(obj)) {
                    list.add(iface.cast(obj));
                }
            }
            return list;
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

            // For the default factory method, which returns a class,
            // associate that class with this refClass.
            Method[] methods = sortMethods(clazz.getMethods());
            for (int i = 0; i < methods.length; i++) {
                Method method = methods[i];
                String methodName = method.getName();
                if (methodName.startsWith("create")
                    && method.getParameterTypes().length == 0)
                {
                    Class instanceClass = method.getReturnType();
                    classMap.put(instanceClass, (RefClass) wrap());
                }
            }
        }

        protected Object proxyCreate(
            String attrName, Object[] args, Class createClass)
        {
            assert args == null;
            return createImpl(createClass, false).wrap();
        }
    }

    /**
     * Specialized handler for implementing a {@link RefAssociation} via a
     * dynamic proxy.
     */
    protected class RefAssociationImpl extends ElementImpl
    {
        RefAssociationImpl(Class clazz)
        {
            super(clazz);
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
                String attrName = parseGetter(methodName);
                if ((attrName != null) &&
                    method.getParameterTypes().length == 0) {
                    Class attributeClass = method.getReturnType();
                    // Attribute is class, package, or association.
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
        boolean compositeParent;
        boolean compositeChild;

        Relationship(Class clazz, String name, boolean many)
        {
            super();    
            this.clazz = clazz;
            this.name = name;
            this.many = many;
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
            super.add(source.wrap());
        }
    }
}

// End JmiMemFactory.java
