/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
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

import java.lang.reflect.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

import org.jgrapht.DirectedGraph;


/**
 * JmiMemFactory creates objects for use in an in-memory repository
 * implementation.
 *
 * @author Julian Hyde
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class JmiMemFactory
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Float FloatZero = Float.valueOf(0);
    private static final Double DoubleZero = Double.valueOf(0);

    private static final Map<String, MethodId> methodMap =
        new HashMap<String, MethodId>();
    static {
        for (int i = 0; i < MethodId.values().length; i++) {
            MethodId methodId = MethodId.values()[i];
            methodMap.put(methodId.name(), methodId);
        }
    }

    //~ Enums ------------------------------------------------------------------

    /**
     * Enumeration of common method names to be handled by the proxy.
     */
    private static enum MethodId
    {
        /** Method {@link Object#toString()}. */
        toString,

        /** Method {@link Object#hashCode()}. */
        hashCode,

        /** Method {@link Object#equals(Object)}. */
        equals,

        /** Method {@link Comparable#compareTo(Object)}. */
        compareTo,

        /** Method {@link RefBaseObject#refMofId()}. */
        refMofId,

        /** Method {@link Element#impl()}. */
        impl,

        /** Method {@link RefObject#refClass()}. */
        refClass,

        /** Method {@link RefPackage#refAllPackages()}. */
        refImmediateComposite,

        /** Method {@link RefObject#refImmediateComposite()}. */
        refOutermostPackage,

        /** Method {@link RefObject#refOutermostPackage()}. */
        refImmediatePackage,

        /** Method {@link RefObject#refImmediatePackage()}. */
        refAllPackages,

        /** Method {@link RefPackage#refAllClasses()}. */
        refAllClasses,

        /** Method {@link RefPackage#refAllAssociations()}. */
        refAllAssociations,

        /** Method {@link RefBaseObject#refMetaObject()}. */
        refMetaObject,

        /** Methods {@link RefClass#refGetEnum(RefObject, String)},
         * {@link RefClass#refGetEnum(String, String)},
         * {@link RefPackage#refGetEnum(RefObject, String)} and
         * {@link RefPackage#refGetEnum(String, String)}. */
        refGetEnum,

        /** Methods {@link RefFeatured#refGetValue(RefObject)} and
         * {@link RefFeatured#refGetValue(String)}. */
        refGetValue,

        /** Methods {@link RefFeatured#refSetValue(RefObject, Object)} and
         * {@link RefFeatured#refSetValue(String, Object)}. */
        refSetValue,

        /** Methods {@link RefPackage#refPackage(RefObject)} and
         * {@link RefPackage#refPackage(String)}. */
        refPackage,

        /** Methods {@link RefPackage#refAssociation(RefObject)} and
         * {@link RefPackage#refAssociation(String)}. */
        refAssociation,

        /** Method {@link RefPackage#refDelete()}. */
        refDelete,

        /** Method {@link RefAssociation#refAllLinks()}. */
        refAllLinks,

        /** Methods {@link RefAssociation#refQuery(RefObject, RefObject)} and
         * {@link RefAssociation#refQuery(String, RefObject)}. */
        refQuery,

        /** Method
         * {@link RefAssociation#refLinkExists(RefObject, RefObject)}. */
        refLinkExists,

        /** Method {@link RefAssociation#refAddLink(RefObject, RefObject)}. */
        refAddLink,

        /** Method
         * {@link RefAssociation#refRemoveLink(RefObject, RefObject)}. */
        refRemoveLink,

        /** Method {@link RefClass#refCreateInstance(List)}. */
        refCreateInstance,

        /** Method {@link RefObject#refIsInstanceOf(RefObject, boolean)}. */
        refIsInstanceOf;
    }

    //~ Instance fields --------------------------------------------------------

    private final AtomicLong nextId;
    private final RefPackageImpl rootPackageImpl;
    private final Map<String, Relationship> relationshipMap;
    private final HashMap<Class<? extends RefBaseObject>, RefObject> metaMap;
    private final Map<RefObject, RefPackage> pluginPackageMap;
    private final Map<Class, RefClass> classMap;

    //~ Constructors -----------------------------------------------------------

    public JmiMemFactory()
    {
        nextId = new AtomicLong(0);
        relationshipMap = new HashMap<String, Relationship>();
        metaMap = new HashMap<Class<? extends RefBaseObject>, RefObject>();
        pluginPackageMap = new HashMap<RefObject, RefPackage>();
        classMap = new HashMap<Class, RefClass>();
        rootPackageImpl = newRootPackage();
    }

    //~ Methods ----------------------------------------------------------------

    public RefPackage getRootPackage()
    {
        return (RefPackage) rootPackageImpl.wrap();
    }

    /**
     * Associates the MOFID of a persistent object with an in-memory object.
     * This is useful when the in-memory object is being manipulated as a shadow
     * of the persistent object.
     *
     * @param obj in-memory object
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
     * as a child of the root package. This is required for plugin sub-packages,
     * which don't have corresponding accessor methods on the root package.
     *
     * @param ifacePackage interface corresponding to RefPackage
     *
     * @return new RefPackage
     */
    public RefPackage newRefPackage(Class ifacePackage)
    {
        ElementImpl impl = createImpl(ifacePackage, rootPackageImpl, true);
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
     * Notifies subclasses that a new object has been created with the given
     * MOFID.
     *
     * @param refObj new object
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
     * Creates the right kind of implementation class to implement the given
     * interface.
     */
    protected ElementImpl createImpl(
        Class<? extends RefBaseObject> clazz,
        RefPackageImpl immediatePkg,
        boolean preemptive)
    {
        if (RefClass.class.isAssignableFrom(clazz)) {
            return new RefClassImpl(
                (Class<? extends RefClass>) clazz,
                immediatePkg);
        } else if (RefPackage.class.isAssignableFrom(clazz)) {
            return new RefPackageImpl(
                (Class<? extends RefPackage>) clazz,
                immediatePkg);
        } else if (RefAssociation.class.isAssignableFrom(clazz)) {
            return new RefAssociationImpl(
                (Class<? extends RefAssociation>) clazz,
                immediatePkg);
        } else {
            if (preemptive) {
                return null;
            } else {
                return new ElementImpl(clazz, immediatePkg);
            }
        }
    }

    protected static Method [] sortMethods(Method [] methods)
    {
        Method [] sortedMethods = (Method []) methods.clone();
        Arrays.sort(
            sortedMethods,
            new Comparator<Method>() {
                public int compare(Method m1, Method m2)
                {
                    return m1.getName().compareTo(m2.getName());
                }
            });

        return sortedMethods;
    }

    /**
     * Looks up a relationship.
     *
     * @param fromClass Name class the relationship is from
     * @param fromName Name of the relationship
     *
     * @return Name of backward relationship, or null if not found
     */
    private Relationship lookupRelationship(
        Class<? extends RefBaseObject> fromClass,
        String fromName)
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
        Class<? extends RefObject> fromClass,
        String fromName,
        boolean fromMany,
        Class<? extends RefObject> toClass,
        String toName,
        boolean toMany)
    {
        createRelationship(
            fromClass,
            fromName,
            fromMany,
            toClass,
            toName,
            toMany,
            false);
    }

    /**
     * Defines a meta-object.
     *
     * @param iface Java interface to associate with meta-object
     * @param metaObject meta-object to return for {@link
     * RefBaseObject#refMetaObject} from instances of iface
     */
    public void defineMetaObject(
        Class<? extends RefBaseObject> iface,
        RefObject metaObject)
    {
        metaMap.put(iface, metaObject);
    }

    /**
     * Creates a relationship definition
     */
    public void createRelationship(
        Class<? extends RefBaseObject> fromClass,
        String fromName,
        boolean fromMany,
        Class<? extends RefBaseObject> toClass,
        String toName,
        boolean toMany,
        boolean composite)
    {
        Relationship relationship1 =
            new Relationship(
                fromClass,
                fromName,
                fromMany);
        Relationship relationship2 =
            new Relationship(
                toClass,
                toName,
                toMany);
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

    protected JmiModelGraph getModelGraph()
    {
        return null;
    }

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * Implemented by proxy objects, this interface makes the proxy handler
     * object accessible.
     */
    interface Element
    {
        ElementImpl impl();
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Implementation of a {@link RefBaseObject} via an {@link
     * InvocationHandler} interface.
     *
     * <p>Attributes are held in a {@link TreeMap}. (Not a {@link HashMap},
     * because we want the attributes to be returned in a predictable order.)
     */
    protected class ElementImpl
        extends TreeMap<String, Object>
        implements InvocationHandler
    {
        protected final Class<? extends RefBaseObject> clazz;
        private final long id;
        private final Object proxy;
        protected final RefPackageImpl immediatePkg;
        String persistentMofId;

        ElementImpl(
            Class<? extends RefBaseObject> clazz,
            RefPackageImpl immediatePkg)
        {
            this.clazz = clazz;
            this.immediatePkg = immediatePkg;
            this.id = nextId.getAndIncrement();
            Method [] methods = clazz.getMethods();

            // Initialize all collections.
            for (int i = 0; i < methods.length; i++) {
                Method method = methods[i];
                String methodName = method.getName();
                Class methodReturn = method.getReturnType();
                String attrName = parseGetter(methodName);

                // TODO jvs 31-Jan-2006: Handle 1-to-1 associations like
                // FarragoConfiguresFennel.  There's no collection
                // involved, but a bidirectional link is still required.
                if ((attrName != null)
                    && (method.getParameterTypes().length == 0))
                {
                    if (Collection.class.isAssignableFrom(methodReturn)) {
                        initCollection(attrName);
                    }
                }
            }
            proxy =
                Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class[] { clazz, Element.class },
                    this);
            mapMofId((RefBaseObject) proxy,
                proxyRefMofId());
        }

        private void initCollection(final String collectionName)
        {
            Relationship relationship =
                lookupRelationship(clazz, collectionName);
            if (relationship == null) {
                put(
                    collectionName,
                    new ArrayList());
            } else if (relationship.inverse.many) {
                put(
                    collectionName,
                    new ManyList(this, relationship));
            } else {
                put(
                    collectionName,
                    new OneWayList(this, relationship));
            }
        }

        public Object invoke(
            Object proxy,
            Method method,
            Object [] args)
            throws Throwable
        {
            // Rather than compare the name of the invoked method with a
            // succession of candidate method names, we convert the method name
            // into an enum and use that enum to switch.
            String name = method.getName();
            MethodId methodId = methodMap.get(name);
            if (methodId != null) {
                // it's a standard method
                switch (methodId) {
                case toString:

                    // Intercept methods on Object. Otherwise we loop.
                    assert method.getDeclaringClass() == Object.class;
                    return clazz + ":" + System.identityHashCode(proxy);
                case hashCode:

                    // Intercept methods on Object. Otherwise we loop.
                    assert method.getDeclaringClass() == Object.class;
                    return new Integer(System.identityHashCode(proxy));
                case equals:

                    // Intercept methods on Object. Otherwise we loop.
                    assert method.getDeclaringClass() == Object.class;
                    return Boolean.valueOf(proxy == args[0]);
                case compareTo:
                    if (method.getDeclaringClass() == Comparable.class) {
                        assert name.equals("compareTo");
                        assert args == null;
                        RefBaseObject that = (RefBaseObject) args[0];
                        String thisMofId = this.proxyRefMofId();
                        String thatMofId = that.refMofId();
                        return new Integer(thisMofId.compareTo(thatMofId));
                    }
                    break;
                case refMofId:
                    assert method.getDeclaringClass() == RefBaseObject.class;
                    assert args == null;
                    return proxyRefMofId();
                case impl:
                    assert method.getDeclaringClass() == Element.class;
                    assert args == null;
                    return proxyImpl();
                case refClass:
                    if (args == null) {
                        return classMap.get(clazz);
                    } else {
                        return proxyRefByMoniker(args[0]);
                    }
                case refImmediateComposite:
                    return proxyImmediateComposite();
                case refOutermostPackage:
                    return rootPackageImpl.wrap();
                case refImmediatePackage:
                    return proxyRefImmediatePackage();
                case refAllPackages:
                    return proxyRefAllPackages();
                case refAllClasses:
                    return filterChildren(RefClass.class);
                case refAllAssociations:
                    return filterChildren(RefAssociation.class);
                case refMetaObject:
                    RefObject obj = metaMap.get(clazz);
                    if (obj != null) {
                        return obj;
                    }
                    return createImpl(MofPackage.class, null, false).wrap();
                case refGetEnum:
                    return proxyRefGetEnum(args[0], (String) args[1]);
                case refGetValue:
                    return proxyRefByMoniker(args[0]);
                case refSetValue:
                    return proxyRefSetValue(args[0], args[1]);
                case refPackage:
                    return proxyRefPackage(args[0]);
                case refAssociation:
                    return proxyRefByMoniker(args[0]);
                case refDelete:

                    // REVIEW jvs 9-Mar-2006: this is to allow code to be
                    // reusable across persistent and mem repositories, but the
                    // behavior will be different!
                    return null;
                case refAllLinks:

                    // REVIEW jvs 30-Jan-2006:  To implement this, we
                    // would have to keep track of extents, which we don't
                    // want to do.  Instead of failing, return empty set
                    // so that XMI export can work (assuming the model
                    // does not contain an association without a corresponding
                    // reference on at least one side).
                    return Collections.EMPTY_SET;
                case refQuery:

                    // REVIEW: swz 20-Nov-2006: Same problem as refAllLinks:
                    // no extents, so we just return an empty collection.
                    return Collections.EMPTY_SET;
                case refLinkExists:
                    return proxyRefLinkExists(
                        (RefObject) args[0],
                        (RefObject) args[1]);
                case refAddLink:
                    return proxyRefAddLink(
                        (RefObject) args[0],
                        (RefObject) args[1]);
                case refRemoveLink:
                    return proxyRefRemoveLink(
                        (RefObject) args[0],
                        (RefObject) args[1]);
                case refCreateInstance:
                    return proxyRefCreateInstance((List) args[0]);
                case refIsInstanceOf:
                    return proxyRefIsInstanceOf(
                        (RefObject) args[0],
                        (Boolean) args[1]);
                }
            }

            // non-standard methods
            String getter = parseGetter(name);
            if (getter != null) {
                assert args == null;
                return proxyGet(
                    getter,
                    method,
                    args);
            } else if (name.startsWith("set")) {
                return proxySet(
                    name.substring(3),
                    method,
                    args);
            } else if (name.startsWith("create")) {
                return proxyCreate(
                    name.substring(6),
                    args,
                    method.getReturnType());
            }
            throw new UnsupportedOperationException(method.toString());
        }

        protected String proxyRefMofId()
        {
            // Radix 16 is important: see JmiObjUtil.getObjectId(), which
            // converts the id back into a long.  If someone then reproduces
            // the String and does a lookup it won't work if the radix doesn't
            // match.
            return "x:" + Long.toString(id, 16);
        }

        protected Object proxyCreate(
            String attrName,
            Object [] args,
            Class createClass)
        {
            throw new UnsupportedOperationException();
        }

        protected Object proxySet(
            String attrName,
            Method method,
            Object [] args)
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
            String attrName,
            Object value,
            boolean needRelationshipCheck)
        {
            if (value instanceof Collection) {
                Collection oldVal = (Collection) get(attrName);

                // REVIEW jvs 30-Nov-2006:  need to break existing links?
                oldVal.clear();
                for (Object o : (Collection) value) {
                    oldVal.add(o);
                }
                return null;
            }
            if (needRelationshipCheck) {
                final Relationship relationship =
                    lookupRelationship(
                        clazz,
                        attrName);

                if (relationship != null) {
                    Object oldVal = get(attrName);
                    if (oldVal != null) {
                        // Break association with existing partner.
                        ElementImpl oldPartner = ((Element) oldVal).impl();
                        if (relationship.inverse.many) {
                            OneWayList inverseCollection =
                                (OneWayList) oldPartner.get(
                                    relationship.inverse.name);
                            inverseCollection.remove(proxy);
                        } else {
                            oldPartner.put(
                                relationship.inverse.name,
                                null);
                        }
                    }

                    if (value != null) {
                        // Add the object at the other end.
                        final ElementImpl elementImpl =
                            ((Element) value).impl();
                        if (relationship.inverse.many) {
                            OneWayList inverseCollection =
                                (OneWayList) elementImpl.get(
                                    relationship.inverse.name);
                            inverseCollection.add(proxy);
                        } else {
                            elementImpl.put(
                                relationship.inverse.name,
                                proxy);
                        }
                    }
                }
            }
            return put(attrName, value);
        }

        protected Object proxyGet(
            String attrName,
            Method method,
            Object [] args)
        {
            Class attrClass = method.getReturnType();
            assert args == null;
            Object o = get(attrName);
            if ((o == null) && attrClass.isPrimitive()) {
                // Primitive values cannot be null. So, provide a value.
                if (attrClass == int.class) {
                    return new Integer(0);
                } else if (attrClass == long.class) {
                    return Long.valueOf(0);
                } else if (attrClass == boolean.class) {
                    return Boolean.FALSE;
                } else if (attrClass == double.class) {
                    return DoubleZero;
                } else if (attrClass == float.class) {
                    return FloatZero;
                } else {
                    throw Util.newInternal(attrClass.getName());
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

        protected RefPackage proxyRefImmediatePackage()
        {
            if (immediatePkg == null) {
                // Outermost package
                return null;
            }

            // Have to cast RefPackageImpl to ElementImpl to get at the
            // proxy field.  (Eclipse, at least, doesn't allow this otherwise.)
            return (RefPackage) ((ElementImpl) immediatePkg).proxy;
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
            Object result =
                proxySet(
                    parseGetter(accessorName),
                    value,
                    true);
            return result;
        }

        protected String getAccessorName(Object moniker)
        {
            if (moniker instanceof String) {
                String name = (String) moniker;

                return "get" + Character.toUpperCase(name.charAt(0))
                    + name.substring(1);
            } else {
                ModelElement modelElement = (ModelElement) moniker;
                return JmiObjUtil.getAccessorName(modelElement);
            }
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
                return 
                    obj != null
                        ? obj.equals(rr.referencedEnd)
                        : rr.referencedEnd == null;
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
                Collection c =
                    (Collection) rr.referencingEnd.refGetValue(rr.ref);
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
                Collection c =
                    (Collection) rr.referencingEnd.refGetValue(rr.ref);
                c.remove(rr.referencedEnd);
            } else {
                rr.referencingEnd.refSetValue(rr.ref, null);
            }
            return Boolean.TRUE;
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
            for (
                Reference featureRef
                : JmiObjUtil.getFeatures(
                    rr.referencingEnd.refClass(),
                    Reference.class,
                    rr.multiValued))
            {
                if (featureRef.getReferencedEnd().equals(referencedEnd)) {
                    rr.ref = featureRef;
                    return true;
                }
            }
            return false;
        }

        protected Object proxyRefCreateInstance(List args)
        {
            Class createClass = null;
            Method [] methods = clazz.getMethods();
            for (int i = 0; i < methods.length; ++i) {
                if (!methods[i].getName().startsWith("create")) {
                    continue;
                }
                createClass = methods[i].getReturnType();
                break;
            }
            assert (createClass != null);
            ElementImpl impl = createImpl(createClass, immediatePkg, false);

            Iterator featureIter =
                JmiObjUtil.getFeatures((RefClass) wrap(),
                    Attribute.class,
                    true).iterator();

            for (Object arg : args) {
                StructuralFeature feature =
                    (StructuralFeature) featureIter.next();
                impl.proxySet(
                    parseGetter(JmiObjUtil.getAccessorName(feature)),
                    arg,
                    true);
            }
            return impl.wrap();
        }

        protected Boolean proxyRefIsInstanceOf(
            RefObject refObject,
            boolean considerSubTypes)
        {
            MofClass thisMofClass = (MofClass)
                classMap.get(clazz).refMetaObject();

            return isInstanceOf(thisMofClass, refObject, considerSubTypes);
        }

        private boolean isInstanceOf(
            MofClass mofClass,
            RefObject refObject,
            boolean considerSubTypes)
        {
            if (refObject.equals(mofClass)) {
                return true;
            }

            if (!considerSubTypes) {
                return false;
            }

            // TODO jvs 25-Mar-2008:  provide access to JmiModelView
            // and use getAllSubclassVertices for faster test
            JmiModelGraph modelGraph = getModelGraph();

            // Some tests simply use JmiMemFactory directly, in which case
            // there's no model graph to be had.  However, these tests all
            // worked before this method was implemented, so they clearly
            // don't require it.  If you hit this assertion, you've added
            // a test that needs this method and will have to rewrite your
            // test case to use JmiModeledMemFactory (see JmiMemTest).
            assert (modelGraph != null);

            JmiClassVertex mofClassVertex =
                modelGraph.getVertexForMofClass(mofClass);

            // Traverse up mofClass's inheritance chain and see if we find
            // a match.
            DirectedGraph<JmiClassVertex, JmiInheritanceEdge> inheritanceGraph =
                modelGraph.getInheritanceGraph();

            Set<JmiInheritanceEdge> edges =
                inheritanceGraph.incomingEdgesOf(mofClassVertex);
            for (JmiInheritanceEdge edge : edges) {
                mofClass = edge.getSuperClass().getMofClass();

                if (isInstanceOf(mofClass, refObject, true)) {
                    return true;
                }
            }

            return false;
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

        private class ResolvedReference
        {
            Reference ref;
            RefObject referencingEnd;
            RefObject referencedEnd;
            boolean multiValued;
        }
    }

    /**
     * Specialized handler for implementing a {@link RefClass} via a dynamic
     * proxy.
     */
    protected class RefClassImpl
        extends ElementImpl
    {
        RefClassImpl(
            Class<? extends RefClass> clazz,
            RefPackageImpl immediatePkg)
        {
            super(clazz, immediatePkg);

            // For the default factory method, which returns a class,
            // associate that class with this refClass.
            Method [] methods = sortMethods(clazz.getMethods());
            for (int i = 0; i < methods.length; i++) {
                Method method = methods[i];
                String methodName = method.getName();
                if (methodName.startsWith("create")
                    && (method.getParameterTypes().length == 0))
                {
                    Class instanceClass = method.getReturnType();
                    classMap.put(instanceClass, (RefClass) wrap());
                }
            }
        }

        protected Object proxyCreate(
            String attrName,
            Object [] args,
            Class createClass)
        {
            assert args == null;
            return createImpl(createClass, immediatePkg, false).wrap();
        }
    }

    /**
     * Specialized handler for implementing a {@link RefAssociation} via a
     * dynamic proxy.
     */
    protected class RefAssociationImpl
        extends ElementImpl
    {
        RefAssociationImpl(
            Class<? extends RefAssociation> clazz,
            RefPackageImpl immediatePkg)
        {
            super(clazz, immediatePkg);
        }
    }

    /**
     * Specialized handler for implementing a {@link RefPackage} via a dynamic
     * proxy.
     */
    protected class RefPackageImpl
        extends ElementImpl
    {
        public RefPackageImpl(Class<? extends RefPackage> clazz)
        {
            this(clazz, null);
        }

        public RefPackageImpl(
            Class<? extends RefPackage> clazz,
            RefPackageImpl immediatePkg)
        {
            super(clazz, immediatePkg);

            // For each method which returns a class, create an attribute.
            Method [] methods = sortMethods(clazz.getMethods());
            for (int i = 0; i < methods.length; i++) {
                Method method = methods[i];
                String methodName = method.getName();
                String attrName = parseGetter(methodName);
                if ((attrName != null)
                    && (method.getParameterTypes().length == 0))
                {
                    Class attributeClass = method.getReturnType();

                    // Attribute is class, package, or association.
                    ElementImpl el = createImpl(attributeClass, this, true);
                    if (el != null) {
                        put(
                            attrName,
                            el.wrap());
                    }
                }
            }
        }
    }

    protected class MofPackageImpl
        extends ElementImpl
    {
        MofPackageImpl(
            Class<? extends RefBaseObject> clazz,
            RefPackageImpl immediatePkg)
        {
            super(clazz, immediatePkg);
        }
    }

    /**
     * Definition of a relationship.
     */
    static class Relationship
    {
        final Class<? extends RefBaseObject> clazz;
        final String name;
        final boolean many;

        Relationship inverse;
        boolean compositeParent;
        boolean compositeChild;

        Relationship(
            Class<? extends RefBaseObject> clazz,
            String name,
            boolean many)
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
    private static class OneWayList
        extends ArrayList<Object>
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

        public boolean addAll(Collection<? extends Object> c)
        {
            for (Object o : c) {
                this.add(o);
            }

            return c.size() > 0;
        }
    }

    /**
     * List which holds instances of a bi-directional relationship.
     *
     * <p>When an instance of the relationship is created, by calling {@link
     * #add(Object)} to the collection at one end, this collection automatically
     * finds the corresponding collection at the other end and calls its <code>
     * addInternal</code> method.
     */
    private static class ManyList
        extends ArrayList<Object>
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
            if (inverseCollection != null) {
                inverseCollection.addInternal(element);
            }
            return super.add(o);
        }

        public boolean addAll(Collection<? extends Object> c)
        {
            for (Object o : c) {
                this.add(o);
            }

            return c.size() > 0;
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
