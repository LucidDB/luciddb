/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.util;

import net.sf.saffron.util.*;

import org.netbeans.api.xmi.*;
import org.netbeans.lib.jmi.util.*;

import java.io.*;

import java.util.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;


/**
 * Static JMI utilities.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class JmiUtil
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Gets a SortedMap (from String to Object) containing the attribute values
     * for a RefObject.  Multi-valued attributes are not included.
     *
     * @param src RefObject to query
     *
     * @return map with attribute names as ordering keys
     */
    public static SortedMap getAttributeValues(RefObject src)
    {
        RefClass refClass = src.refClass();
        Iterator iter = getFeatures(refClass,Attribute.class,false).iterator();
        SortedMap map = new TreeMap();
        while (iter.hasNext()) {
            Attribute attr = (Attribute) iter.next();
            map.put(attr.getName(),src.refGetValue(attr));
        }
        return map;
    }

    /**
     * Gets a List of instance-level StructuralFeatures for a RefClass.
     *
     * @param refClass class of interest
     *
     * @param filterClass only objects which are instances of this Class will
     * be returned; so, for example, pass Attribute.class if you want only
     * attributes, or StructuralFeature.class if you want everything
     *
     * @param includeMultiValued if true, multi-valued attributes will
     * be included; otherwise, they will be filtered out
     *
     * @return attribute list
     */
    public static List getFeatures(
        RefClass refClass,Class filterClass,boolean includeMultiValued)
    {
        assert(StructuralFeature.class.isAssignableFrom(filterClass));
        List list = new ArrayList();
        MofClass mofClass = (MofClass) refClass.refMetaObject();
        List superList = mofClass.allSupertypes();
        Iterator iter = superList.iterator();
        while (iter.hasNext()) {
            MofClass mofSuper = (MofClass) iter.next();
            addFeatures(list,mofSuper,filterClass,includeMultiValued);
        }
        addFeatures(list,mofClass,filterClass,includeMultiValued);
        return list;
    }

    private static void addFeatures(
        List list,MofClass mofClass,Class filterClass,
        boolean includeMultiValued)
    {
        Iterator iter = mofClass.getContents().iterator();
        while (iter.hasNext()) {
            Object obj = iter.next();
            if (!(filterClass.isInstance(obj))) {
                continue;
            }
            StructuralFeature feature = (StructuralFeature) obj;
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
    public static void copyAttributes(RefObject dst,RefObject src)
    {
        RefClass refClass = src.refClass();
        MofClass mofClass = (MofClass) refClass.refMetaObject();
        Iterator iter = mofClass.getContents().iterator();
        while (iter.hasNext()) {
            Object obj = iter.next();
            if (!(obj instanceof Attribute)) {
                continue;
            }
            Attribute attr = (Attribute) obj;
            if (!(attr.getScope().equals(ScopeKindEnum.INSTANCE_LEVEL))) {
                continue;
            }
            dst.refSetValue(attr,src.refGetValue(attr));
        }
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
            xmiWriter.write(outStream,collection,"1.2");
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
     * @return imported JMI objects
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
     *        composite types
     *
     * @return cloned instance
     */
    public static RefObject newClone(RefObject refObject)
    {
        RefClass refClass = refObject.refClass();
        MofClass mofClass = (MofClass) refClass.refMetaObject();
        RefObject cloned = refClass.refCreateInstance(Collections.EMPTY_LIST);
        copyAttributes(cloned,refObject);
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
        // TODO:  Use the proper JMI metadata access.  This hack is dependent
        // on the way MDR names the implementation of the metaclass.
        String className = refClass.getClass().getName();
        String classSuffix = "Class$Impl";
        assert (className.endsWith(classSuffix));
        className = className.substring(
            0,className.length() - classSuffix.length());
        return Class.forName(className);
    }

    /**
     * Gets the 64-bit object ID for a JMI object.  This is taken from the last
     * 8 bytes of the MofId.  REVIEW: need to make sure this is locally unique
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
        assert(colonPos > -1);
        return Long.parseLong(mofId.substring(colonPos+1),16);
    }

    /**
     * Looks up a subpackage by name.
     *
     * @param rootPackage starting package from which to descend
     *
     * @param names array of package names representing path
     *
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
    public static String getMetaObjectName(
        RefBaseObject refObject)
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
     * @param typedElement TypedElement to be accessed
     *
     * @return constructed accessor name
     */
    public static String getAccessorName(TypedElement typedElement)
    {
        TagProvider tagProvider = new TagProvider();
        String accessorName = tagProvider.getSubstName(typedElement);
        String prefix = null;
        if (typedElement.getType().getName().equals("Boolean")) {
            if (!accessorName.startsWith("is")) {
                prefix = "is";
            }
        } else {
            prefix = "get";
        }
        if (prefix != null) {
            accessorName =
                prefix
                + Character.toUpperCase(accessorName.charAt(0))
                + accessorName.substring(1);
        }
        return accessorName;
    }

    /**
     * Finds the Java class generated for a particular RefClass.
     *
     * @param refClass the reflective JMI class representation
     *
     * @return the generated Java class, or RefObject.class if
     * no Java class has been generated
     */
    public static Class getClassForRefClass(RefClass refClass)
    {
        // Look up the Java interface generated for the class being queried.
        TagProvider tagProvider = new TagProvider();
        String className = tagProvider.getImplFullName(
            (ModelElement) (refClass.refMetaObject()),
            TagProvider.INSTANCE);
        assert(className.endsWith("Impl"));
        className = className.substring(0,className.length() - 4);
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException ex) {
            // This is possible when we're querying an external repository
            // for which we don't know the class mappings.  Do everything
            // via JMI reflection instead.
            return RefObject.class;
        }
    }

    /**
     * Gets an object's container.  If the object is a ModelElement,
     * its container will be too; otherwise, its container will be
     * a RefPackage.
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
     * Asserts that constraints are satisfied.  I tried using
     * refVerifyConstraints to achieve this, but it didn't work in MDR.
     * For now, this just checks that mandatory attributes have
     * non-null values.
     *
     * @param obj the object to be verified
     */
    public static void assertConstraints(RefObject obj)
    {
        RefClass refClass = obj.refClass();
        Iterator featureIter = getFeatures(
            refClass,
            Attribute.class,
            false).iterator();
        while (featureIter.hasNext()) {
            Attribute attr = (Attribute) featureIter.next();
            if (attr.getMultiplicity().getLower() != 0) {
                assert (obj.refGetValue(attr) != null) :
                    "Missing value for mandatory attribute "
                    + ((ModelElement) refClass.refMetaObject()).getName() + "."
                    + attr.getName();
            }
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
