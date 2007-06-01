/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package org.eigenbase.jmi;

import java.io.*;

import java.util.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import org.eigenbase.util.*;

import org.netbeans.api.xmi.*;
import org.netbeans.lib.jmi.util.*;
import org.netbeans.mdr.handlers.*;


/**
 * Static JMI utilities.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class JmiObjUtil
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Gets a SortedMap (from String to Object) containing the attribute values
     * for a RefObject. Multi-valued attributes are not included.
     *
     * @param src RefObject to query
     *
     * @return map with attribute names as ordering keys
     */
    public static SortedMap<String, Object> getAttributeValues(RefObject src)
    {
        RefClass refClass = src.refClass();
        SortedMap<String, Object> map = new TreeMap<String, Object>();
        for (Attribute attr : getFeatures(refClass, Attribute.class, false)) {
            map.put(
                attr.getName(),
                src.refGetValue(attr));
        }
        return map;
    }

    /**
     * Sets values for attributes of a RefObject.
     *
     * @param dst object to modify
     * @param map see return of getAttributeValues
     */
    public static void setAttributeValues(
        RefObject dst,
        SortedMap<String, Object> map)
    {
        RefClass refClass = dst.refClass();
        MofClass mofClass = (MofClass) refClass.refMetaObject();
        for (Attribute attr : getFeatures(refClass, Attribute.class, false)) {
            if (!(attr.getScope().equals(ScopeKindEnum.INSTANCE_LEVEL))) {
                continue;
            }
            if (!(attr.isChangeable())) {
                continue;
            }
            if (!map.containsKey(attr.getName())) {
                continue;
            }
            Object srcVal = map.get(attr.getName());

            Object oldVal = dst.refGetValue(attr);

            if ((oldVal == null) && (srcVal == null)) {
                continue;
            }
            if ((oldVal != null) && (oldVal.equals(srcVal))) {
                continue;
            }

            if (srcVal instanceof RefObject) {
                RefObject srcValObj = (RefObject) srcVal;
                if (srcValObj.refImmediateComposite() != null) {
                    RefObject oldValRef = (RefObject) oldVal;

                    if (oldValRef != null) {
                        if (compositeEquals(oldValRef, srcValObj)) {
                            continue;
                        }
                    }

                    // Trying to copy this directly would lead
                    // to a CompositionViolationException.  Instead,
                    // clone it and reference the clone instead.
                    RefObject clone =
                        srcValObj.refClass().refCreateInstance(
                            Collections.EMPTY_LIST);
                    copyAttributes(clone, srcValObj);
                    srcVal = clone;

                    // Also have to refDelete the old value if any,
                    // otherwise it will become garbage.

                    if (oldValRef != null) {
                        // Nullify reference before deleting oldVal,
                        // otherwise the next refSetValue complains.
                        dst.refSetValue(attr, null);
                        oldValRef.refDelete();
                    }
                }
            }

            dst.refSetValue(
                attr,
                srcVal);
        }
    }

    private static boolean compositeEquals(RefObject obj1, RefObject obj2)
    {
        SortedMap map1 = getAttributeValues(obj1);
        SortedMap map2 = getAttributeValues(obj2);

        return map1.equals(map2);
    }

    /**
     * Gets a List of instance-level StructuralFeatures for a RefClass.
     *
     * @param refClass class of interest
     * @param filterClass only objects which are instances of this Class will be
     * returned; so, for example, pass Attribute.class if you want only
     * attributes, or StructuralFeature.class if you want everything
     * @param includeMultiValued if true, multi-valued attributes will be
     * included; otherwise, they will be filtered out
     *
     * @return attribute list
     */
    public static <T extends StructuralFeature> List<T> getFeatures(
        RefClass refClass,
        Class<T> filterClass,
        boolean includeMultiValued)
    {
        assert (StructuralFeature.class.isAssignableFrom(filterClass));
        List<T> list = new ArrayList<T>();
        MofClass mofClass = (MofClass) refClass.refMetaObject();
        List<MofClass> superTypes =
            new ArrayList<MofClass>(mofClass.allSupertypes());
        superTypes.add(mofClass);
        for (MofClass featuredClass : superTypes) {
            addFeatures(list, featuredClass, filterClass, includeMultiValued);
        }
        return list;
    }

    private static <T extends StructuralFeature> void addFeatures(
        List<T> list,
        MofClass mofClass,
        Class<T> filterClass,
        boolean includeMultiValued)
    {
        for (Object obj : mofClass.getContents()) {
            if (!(filterClass.isInstance(obj))) {
                continue;
            }
            T feature = filterClass.cast(obj);
            if (!(feature.getScope().equals(ScopeKindEnum.INSTANCE_LEVEL))) {
                continue;
            }
            if (!includeMultiValued) {
                if (feature.getMultiplicity().getUpper() != 1) {
                    continue;
                }
            }
            list.add(feature);
        }
    }

    /**
     * Copies attribute values from one RefObject to another compatible
     * RefObject.
     *
     * @param dst RefObject to copy to
     * @param src RefObject to copy from
     */
    public static void copyAttributes(
        RefObject dst,
        RefObject src)
    {
        SortedMap<String, Object> map = getAttributeValues(src);
        setAttributeValues(dst, map);
    }

    /**
     * Exports a collection of JMI objects as XMI.
     *
     * @param collection JMI objects to be exported
     *
     * @return string representation of XMI
     */
    public static String exportToXmiString(Collection collection)
    {
        XMIWriter xmiWriter = XMIWriterFactory.getDefault().createXMIWriter();
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        try {
            xmiWriter.write(outStream, collection, "1.2");
        } catch (IOException ex) {
            throw Util.newInternal(ex);
        }
        return outStream.toString();
    }

    /**
     * Imports a collection of JMI objects from XMI.
     *
     * @param extent target
     * @param string string representation of XMI
     *
     * @return outermost JMI objects imported
     */
    public static Collection importFromXmiString(
        RefPackage extent,
        String string)
    {
        XMIReader xmiReader = XMIReaderFactory.getDefault().createXMIReader();
        try {
            return xmiReader.read(
                new ByteArrayInputStream(string.getBytes()),
                null,
                extent);
        } catch (Exception ex) {
            throw Util.newInternal(ex);
        }
    }

    /**
     * Clones a RefObject.
     *
     * @param refObject RefObject to clone; must have neither associations nor
     * composite types
     *
     * @return cloned instance
     */
    public static RefObject newClone(RefObject refObject)
    {
        RefClass refClass = refObject.refClass();
        MofClass mofClass = (MofClass) refClass.refMetaObject();
        RefObject cloned = refClass.refCreateInstance(Collections.EMPTY_LIST);
        copyAttributes(cloned, refObject);
        return cloned;
    }

    /**
     * Finds the Java interface corresponding to a JMI class.
     *
     * @param refClass the JMI class
     *
     * @return corresponding Java interface
     */
    public static Class getJavaInterfaceForRefClass(RefClass refClass)
        throws ClassNotFoundException
    {
        return getJavaInterfaceForProxy(
            refClass.getClass(),
            "$Impl");
    }

    /**
     * Finds the Java interface corresponding to a JMI association
     *
     * @param refAssoc the JMI association
     *
     * @return corresponding Java interface
     */
    public static Class getJavaInterfaceForRefAssoc(RefAssociation refAssoc)
        throws ClassNotFoundException
    {
        return getJavaInterfaceForProxy(
            refAssoc.getClass(),
            "$Impl");
    }

    /**
     * Finds the Java interface corresponding to object instances of a JMI
     * class.
     *
     * @param refClass the JMI class
     *
     * @return corresponding Java interface
     */
    public static Class getJavaInterfaceForRefObject(RefClass refClass)
        throws ClassNotFoundException
    {
        return getJavaInterfaceForProxy(
            refClass.getClass(),
            "Class$Impl");
    }

    /**
     * Finds the Java interface corresponding to a JMI package.
     *
     * @param refPackage the JMI package
     *
     * @return corresponding Java interface
     */
    public static Class getJavaInterfaceForRefPackage(RefPackage refPackage)
        throws ClassNotFoundException
    {
        return getJavaInterfaceForProxy(
            refPackage.getClass(),
            "$Impl");
    }

    private static Class getJavaInterfaceForProxy(
        Class proxyClass,
        String classSuffix)
        throws ClassNotFoundException
    {
        // REVIEW: This hack is dependent on the way MDR names
        // implementation classes.
        String className = proxyClass.getName();
        assert (className.endsWith(classSuffix));
        className =
            className.substring(0, className.length() - classSuffix.length());
        return Class.forName(
            className,
            true,
            proxyClass.getClassLoader());
    }

    /**
     * Gets the 64-bit object ID for a JMI object. This is taken from the last 8
     * bytes of the MofId. REVIEW: need to make sure this is locally unique
     * within a repository.
     *
     * @param refObject JMI object
     *
     * @return object ID
     */
    public static long getObjectId(RefObject refObject)
    {
        String mofId = refObject.refMofId();
        int colonPos = mofId.indexOf(':');
        assert (colonPos > -1);
        return Long.parseLong(
            mofId.substring(colonPos + 1),
            16);
    }

    /**
     * Looks up a subpackage by name.
     *
     * @param rootPackage starting package from which to descend
     * @param names array of package names representing path
     * @param prefix number of elements of names to use
     *
     * @return subpackage or null if not found
     */
    public static RefPackage getSubPackage(
        RefPackage rootPackage,
        String [] names,
        int prefix)
    {
        try {
            RefPackage pkg = rootPackage;
            for (int i = 0; i < prefix; ++i) {
                pkg = pkg.refPackage(names[i]);
                if (pkg == null) {
                    return null;
                }
            }
            return pkg;
        } catch (InvalidNameException ex) {
            return null;
        }
    }

    /**
     * Gets the name of the model element corresponding to a RefBaseObject
     *
     * @param refObject RefBaseObject representation of a model element
     *
     * @return model element name
     */
    public static String getMetaObjectName(RefBaseObject refObject)
    {
        ModelElement modelElement;
        if (refObject instanceof ModelElement) {
            modelElement = (ModelElement) refObject;
        } else {
            modelElement = (ModelElement) refObject.refMetaObject();
        }
        return modelElement.getName();
    }

    /**
     * Constructs the generated name of an accessor method.
     *
     * @param modelElement ModelElement to be accessed
     *
     * @return constructed accessor name
     */
    public static String getAccessorName(ModelElement modelElement)
    {
        TagProvider tagProvider = new TagProvider();
        String accessorName = tagProvider.getSubstName(modelElement);
        String prefix = "get";
        if (modelElement instanceof TypedElement) {
            TypedElement typedElement = (TypedElement) modelElement;
            if (typedElement.getType().getName().equals("Boolean")) {
                if (!accessorName.startsWith("is")) {
                    prefix = "is";
                } else {
                    prefix = null;
                }
            }
        }
        if (prefix != null) {
            accessorName =
                prefix + Character.toUpperCase(accessorName.charAt(0))
                + accessorName.substring(1);
        }
        return accessorName;
    }

    /**
     * Constructs the generated name of an enum symbol.
     *
     * @param enumSymbol name of enumeration symbol
     *
     * @return constructed field name
     */
    public static String getEnumFieldName(String enumSymbol)
    {
        return TagProvider.mapEnumLiteral(enumSymbol);
    }

    /**
     * Finds the Java class generated for a particular RefClass, or {@link
     * RefObject}.class if not found.
     *
     * @param refClass the reflective JMI class representation
     *
     * @return the generated Java class, or RefObject.class if no Java class has
     * been generated
     */
    public static Class<? extends RefObject> getClassForRefClass(
        RefClass refClass)
    {
        // NOTE jvs 8-Aug-2006:  default to MDR's classloader, otherwise
        // we get visibility problems with generated MDR classes in
        // some contexts
        return getClassForRefClass(
            BaseObjectHandler.getDefaultClassLoader(),
            refClass,
            false);
    }

    /**
     * Finds the Java class generated for a particular RefClass.
     *
     * @param classLoader Class loader. Must not be null: if in doubt, use
     * {@link ClassLoader#getSystemClassLoader()}
     * @param refClass the reflective JMI class representation
     * @param nullIfNotFound If true, return null if not found; if false, return
     * {@link RefObject}.class if not found
     *
     * @return the generated Java class, or RefObject.class if no Java class has
     * been generated
     */
    public static Class<? extends RefObject> getClassForRefClass(
        ClassLoader classLoader,
        RefClass refClass,
        boolean nullIfNotFound)
    {
        assert classLoader != null : "require classLoader: use ClassLoader.getSystemClassLoader()";

        // Look up the Java interface generated for the class being queried.
        TagProvider tagProvider = new TagProvider();
        String className =
            tagProvider.getImplFullName(
                (ModelElement) (refClass.refMetaObject()),
                TagProvider.INSTANCE);
        assert (className.endsWith("Impl"));
        className = className.substring(0, className.length() - 4);

        // hack for MDR MOF implementation
        className =
            className.replaceFirst(
                "org\\.netbeans\\.jmiimpl\\.mof",
                "javax.jmi");
        try {
            return (Class<? extends RefObject>) Class.forName(
                className,
                true,
                classLoader);
        } catch (ClassNotFoundException ex) {
            // This is possible when we're querying an external repository
            // for which we don't know the class mappings.  Do everything
            // via JMI reflection instead.
            return nullIfNotFound ? null : RefObject.class;
        }
    }

    /**
     * Gets an object's container. If the object is a ModelElement, its
     * container will be too; otherwise, its container will be a RefPackage.
     *
     * @param refObject object for which to find the container
     *
     * @return container
     */
    public static RefBaseObject getContainer(RefBaseObject refObject)
    {
        if (refObject instanceof ModelElement) {
            return ((ModelElement) refObject).getContainer();
        } else {
            return refObject.refImmediatePackage();
        }
    }

    /**
     * Asserts that constraints are satisfied. This method exists because
     * refVerifyConstraints didn't used to work MDR. Now it does, so this method
     * is deprecated.
     *
     * @param obj the object to be verified
     *
     * @deprecated use {@link RefBaseObject#refVerifyConstraints} instead
     */
    public static void assertConstraints(RefObject obj)
    {
        // REVIEW (2006/6/3, jhyde): This method noops if constraints are
        // disabled. Change the signature of this method to
        //    boolean constraintsAreValid(RefOject obj, boolean fail)
        // so it is easy to invoke it ONLY if assertions are enabled.

        RefClass refClass = obj.refClass();
        for (
            StructuralFeature feature
            : getFeatures(refClass, StructuralFeature.class, false))
        {
            if (feature.getMultiplicity().getLower() != 0) {
                if (obj.refGetValue(feature) == null) {
                    String featureClassName = getMetaObjectName(refClass);
                    String objectName = null;
                    try {
                        // If it has a name attribute, use that
                        objectName = (String) obj.refGetValue("name");
                    } catch (Throwable ex) {
                        // Otherwise, just dump it
                        objectName = obj.toString();
                    }
                    assert (false) : "Missing value for mandatory feature "
                        + featureClassName + "." + feature.getName()
                        + " in object " + objectName;
                }
            }
        }
    }

    /**
     * Sets default values for mandatory attributes of primitive type (e.g.
     * false for boolean). MDR actually doesn't require this (it synthesizes the
     * default values on demand), except in refVerifyConstraints, so we call
     * this just before invoking that method.
     *
     * @param obj the object being updated
     */
    public static void setMandatoryPrimitiveDefaults(RefObject obj)
    {
        RefClass refClass = obj.refClass();
        for (Attribute attr : getFeatures(refClass, Attribute.class, false)) {
            if (!attr.isChangeable()) {
                continue;
            }
            if (attr.getMultiplicity().getLower() == 0) {
                continue;
            }

            // This is imprecise but does the job.
            Object val = obj.refGetValue(attr);
            if (val == null) {
                // No default available
                continue;
            }
            if (val instanceof RefObject) {
                // Not a primitive
                continue;
            }
            obj.refSetValue(attr, obj.refGetValue(attr));
        }
    }

    /**
     * Tests an attribute value to see if it is blank.
     *
     * @param value
     *
     * @return true if value is either null or the empty string
     */
    public static boolean isBlank(String value)
    {
        return (value == null) || value.equals("");
    }
}

// End JmiUtil.java
