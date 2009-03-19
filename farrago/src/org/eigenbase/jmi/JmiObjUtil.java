/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

import org.eigenbase.enki.mdr.*;
import org.eigenbase.enki.util.*;
import org.eigenbase.util.*;

import org.jgrapht.graph.*;

import org.netbeans.api.mdr.*;
import org.netbeans.api.xmi.*;


/**
 * Static JMI utilities.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class JmiObjUtil
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Default maximum repository string length. Must match the value used at
     * the time the repository was generated.
     */
    private static final int DEFAULT_MAX_STRING_LENGTH = 128;

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
        SortedMap<String, Object> map1 = getAttributeValues(obj1);
        SortedMap<String, Object> map2 = getAttributeValues(obj2);

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
    public static String exportToXmiString(Collection<?> collection)
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
    public static Collection<RefBaseObject> importFromXmiString(
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
    public static Class<? extends RefClass> getJavaInterfaceForRefClass(
        RefClass refClass)
        throws ClassNotFoundException
    {
        return getJavaInterfaceForProxy(
            refClass.getClass(),
            RefClass.class,
            "$");
    }

    /**
     * Finds the Java interface corresponding to a JMI association
     *
     * @param refAssoc the JMI association
     *
     * @return corresponding Java interface
     */
    public static Class<? extends RefAssociation> getJavaInterfaceForRefAssoc(
        RefAssociation refAssoc)
        throws ClassNotFoundException
    {
        return getJavaInterfaceForProxy(
            refAssoc.getClass(),
            RefAssociation.class,
            "$");
    }

    /**
     * Finds the Java interface corresponding to object instances of a JMI
     * class.
     *
     * @param refClass the JMI class
     *
     * @return corresponding Java interface
     */
    public static Class<? extends RefObject> getJavaInterfaceForRefObject(
        RefClass refClass)
        throws ClassNotFoundException
    {
        return getJavaInterfaceForProxy(
            refClass.getClass(),
            RefObject.class,
            "Class$");
    }

    /**
     * Finds the Java interface corresponding to a JMI package.
     *
     * @param refPackage the JMI package
     *
     * @return corresponding Java interface
     */
    public static Class<? extends RefPackage> getJavaInterfaceForRefPackage(
        RefPackage refPackage)
        throws ClassNotFoundException
    {
        return getJavaInterfaceForProxy(
            refPackage.getClass(),
            RefPackage.class,
            "$");
    }

    private static <T> Class<? extends T> getJavaInterfaceForProxy(
        Class<?> proxyClass,
        Class<T> resultClass,
        String delimiter)
        throws ClassNotFoundException
    {
        // REVIEW: This hack is dependent on the way MDR/Enki names
        // implementation classes.  Note that different Enki repository
        // implementations may use different suffixes, hence we allow
        // any string following the delimiter.
        String className = proxyClass.getName();

        int delimPos = className.lastIndexOf(delimiter);
        assert (delimPos > 0); // Must be found and not at start of string

        className = className.substring(0, delimPos);

        return Class.forName(className, true, proxyClass.getClassLoader())
            .asSubclass(resultClass);
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
     * Gets the MofId for a given 64-bit object ID for a JMI object. Generates
     * the MofId from the long without validating whether the associated object
     * exists.
     *
     * @param objectId JMI object id (as from {@link #getObjectId(RefObject)})
     *
     * @return object's MofId
     */
    public static String toMofId(long objectId)
    {
        return MofIdUtil.makeMofIdStr(objectId);
    }

    /**
     * Returns the type name of an object. For example, "FemLocalView".
     *
     * @param refObject Object
     *
     * @return type name
     */
    public static String getTypeName(RefObject refObject)
    {
        return toString(refObject.refClass());
    }

    /**
     * Returns the name of a class.
     *
     * @param refClass Class
     *
     * @return Name of class
     */
    public static String toString(RefClass refClass)
    {
        return refClass.refMetaObject().refGetValue("name").toString();
    }

    /**
     * Generates a string describing an object and its attributes.
     *
     * <p>Useful for debugging. Typical result:
     *
     * <blockquote>FemDataWrapper(creationTimestamp='2007-09-02 16:57:22.252',
     * description='null', foreign='true', language='JAVA', libraryFile='class
     * net.sf.farrago.namespace.mdr.MedMdrForeignDataWrapper',
     * lineageId='1dacd3af-9181-47fd-94f3-64fb3a0506a4',
     * modificationTimestamp='2007-09-02 17:21:08.846', name='SYS_MDR',
     * visibility='vk_public')</code></blockquote>
     *
     * @param refObject Object
     *
     * @return Description of object
     */
    public static String toString(RefObject refObject)
    {
        StringBuilder buf = new StringBuilder();
        buf.append(getTypeName(refObject)).append('(');
        int i = -1;
        SortedMap<String, Object> map = getAttributeValues(refObject);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (++i > 0) {
                buf.append(", ");
            }
            buf.append(entry.getKey()).append("='").append(entry.getValue())
            .append("'");
        }
        buf.append(')');
        return buf.toString();
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
        String accessorName = TagUtil.getSubstName(modelElement);
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
        return TagUtil.mapEnumLiteral(enumSymbol);
    }

    /**
     * Finds the Java class generated for a particular RefClass, or {@link
     * RefObject}.class if not found.
     *
     * @param repos the MDRepository that the given RefClass belongs to
     * @param refClass the reflective JMI class representation
     *
     * @return the generated Java class, or RefObject.class if no Java class has
     * been generated
     */
    public static Class<? extends RefObject> getClassForRefClass(
        MDRepository repos,
        RefClass refClass)
    {
        // NOTE jvs 8-Aug-2006:  default to MDR's classloader, otherwise
        // we get visibility problems with generated MDR classes in
        // some contexts
        return getClassForRefClass(
            ((EnkiMDRepository) repos).getDefaultClassLoader(),
            refClass,
            false);
    }

    /**
     * @deprecated use {@link #getClassForRefClass(MDRepository, RefClass)}
     */
    @Deprecated public static Class<? extends RefObject> getClassForRefClass(
        RefClass refClass)
    {
        // NOTE jvs 8-Aug-2006:  default to MDR's classloader, otherwise
        // we get visibility problems with generated MDR classes in
        // some contexts
        return getClassForRefClass(
            MDRepositoryFactory.getDefaultClassLoader(),
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
        String className = TagUtil.getInterfaceFullName(refClass);

        try {
            Class<?> cls =
                Class.forName(
                    className,
                    true,
                    classLoader);

            return cls.asSubclass(RefObject.class);
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
                    String objectName;
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
            obj.refSetValue(attr, val);
        }
    }

    /**
     * Limits the result of {@link #getFeatures(RefClass, Class, boolean)} to
     * the first feature with the given name.
     *
     * @param refClass class of interest
     * @param filterClass only objects which are instances of this Class will be
     * returned; so, for example, pass Attribute.class if you want only
     * attributes, or StructuralFeature.class if you want everything
     * @param featureName name of the feature to return
     * @param includeMultiValued if true, multi-valued attributes will be
     * included; otherwise, they will be filtered out
     *
     * @return the first feature matching the given parameters or null if none
     * are found
     */
    public static <T extends StructuralFeature> T getNamedFeature(
        RefClass refClass,
        Class<T> filterClass,
        String featureName,
        boolean includeMultiValued)
    {
        for (T t : getFeatures(refClass, filterClass, false)) {
            if (t.getName().equals(featureName)) {
                return t;
            }
        }

        return null;
    }

    public static int getMaxLength(RefClass refClass, Attribute attr)
    {
        Classifier cls = (Classifier) refClass.refMetaObject();

        if (!attr.isChangeable()) {
            return Integer.MAX_VALUE;
        }

        Classifier type = attr.getType();
        if (type instanceof javax.jmi.model.AliasType) {
            type = ((javax.jmi.model.AliasType) type).getType();
        }
        if (!type.getName().equals("String")) {
            return Integer.MAX_VALUE;
        }

        int maxLength =
            TagUtil.findMaxLengthTag(cls, attr, DEFAULT_MAX_STRING_LENGTH);

        return maxLength;
    }

    /**
     * Tests an attribute value to see if it is blank.
     *
     * @param value Value
     *
     * @return true if value is either null or the empty string
     */
    public static boolean isBlank(String value)
    {
        return (value == null) || value.equals("");
    }

    /**
     * Prints a dependency graph to a given writer.
     *
     * @param graph Dependency graph
     * @param pw Writer
     * @param namer Maps JMI objects in the graph to a descriptive string
     */
    public static void dumpGraph(
        JmiDependencyGraph graph,
        PrintWriter pw,
        Namer namer)
    {
        pw.println("Vertices:");
        Map<JmiDependencyVertex, String> vertexIds =
            new HashMap<JmiDependencyVertex, String>();
        for (JmiDependencyVertex vertex : graph.vertexSet()) {
            int vertexId = vertexIds.size();
            RefObject first = vertex.getElementSet().iterator().next();
            vertexIds.put(vertex, vertexId + ": " + namer.getName(first));
            pw.println("\tVertex #" + vertexId + ":");
            for (RefObject refObject : vertex.getElementSet()) {
                pw.println("\t\t" + namer.getName(refObject));
            }
        }
        pw.println("Edges:");
        for (DefaultEdge edge : graph.edgeSet()) {
            JmiDependencyVertex sourceVertex = graph.getEdgeSource(edge);
            JmiDependencyVertex targetVertex = graph.getEdgeTarget(edge);
            pw.println(
                "\t"
                + vertexIds.get(sourceVertex)
                + " : "
                + vertexIds.get(targetVertex));
        }
    }

    /**
     * Prints a model view to a given writer.
     *
     * @param view Model view
     * @param pw Writer
     */
    public static void dumpGraph(
        JmiModelView view,
        PrintWriter pw)
    {
        pw.println("Vertices:");
        final JmiModelGraph graph = view.getModelGraph();
        Map<JmiClassVertex, String> vertexIds =
            new HashMap<JmiClassVertex, String>();
        for (JmiClassVertex vertex : graph.vertexSet()) {
            int vertexId = vertexIds.size();
            final String vertexDesc =
                vertexId + ": " + toString(vertex.getRefClass());
            vertexIds.put(vertex, vertexDesc);
            pw.println("\tVertex #" + vertexDesc);
        }
        pw.println("Edges:");
        for (DefaultEdge edge : graph.edgeSet()) {
            JmiClassVertex sourceVertex = graph.getEdgeSource(edge);
            JmiClassVertex targetVertex = graph.getEdgeTarget(edge);
            pw.println(
                "\t"
                + vertexIds.get(sourceVertex)
                + " - "
                + vertexIds.get(targetVertex)
                + " (" + edge + ")");
        }
    }

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * Generates a name for a JMI element.
     *
     * <p>This is an interface because the name often depends upon the details
     * the metamodel, and different applications call for different levels of
     * verbosity. Typically people choose to identify an object by its type and
     * its path to the root element, for example
     * "LocalTable(MYCATALOG.MYSCHEMA.MYTABLE)".
     */
    public interface Namer
    {
        /**
         * Returns a string to which identifies a given element to an end-user.
         *
         * @param o Element
         *
         * @return Descriptor
         */
        String getName(RefObject o);
    }
}

// End JmiObjUtil.java
