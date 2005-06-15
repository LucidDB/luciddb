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

import net.sf.farrago.FarragoMetadataFactory;
import net.sf.farrago.FarragoPackage;
import net.sf.farrago.FarragoMetadataFactoryImpl;

import javax.jmi.reflect.RefBaseObject;
import javax.jmi.reflect.RefClass;
import javax.jmi.reflect.RefPackage;
import java.lang.reflect.*;
import java.util.*;
import java.util.regex.Pattern;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.eigenbase.util.Util;
import org.eigenbase.xom.XMLOutput;

/**
 * Mock implementation of {@link FarragoMetadataFactory} which implements
 * the MDR interfaces using dynamic proxies. Since this implementation
 * does not persist objects, or even require a repository instance, it is
 * ideal for testing purposes.
 *
 * <p>MockMetadataFactory uses dynamic proxies (see {@link Proxy}) to
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
public class MockMetadataFactory extends FarragoMetadataFactoryImpl
{
    private int nextId = 0;

    public MockMetadataFactory()
    {
        super();
        RefPackageImpl rootPackageImpl =
            new RefPackageImpl(FarragoPackage.class);
        this.setRootPackage((FarragoPackage) rootPackageImpl.wrap());
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

    /**
     * Converts an object and its sub-objects to an XML string.
     *
     * <p>Note: The XML string isn't quite XMI.
     */
    public static String exportAsXml(RefBaseObject ref)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        new JmiPrinter(pw).accept(ref);
        pw.flush();
        return sw.toString();
    }

    /**
     * Implementation of a {@link RefBaseObject} via an
     * {@link InvocationHandler} interface.
     *
     * <p>Attributes are held in a {@link TreeMap}. (Not a {@link HashMap},
     * because we want the attributes to be returned in a predictable order.)
     */
    protected class ElementImpl
        extends TreeMap
        implements InvocationHandler
    {
        protected final Class clazz;
        private final int id;

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
                    method.getParameterTypes().length == 0 &&
                    Collection.class.isAssignableFrom(methodReturn)) {
                    put(method.getName().substring(3), new ArrayList());
                }
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
            } else if (methodName.startsWith("get")) {
                assert args == null;
                return proxyGet(methodName.substring(3), method, args);
            } else if (methodName.startsWith("set")) {
                return proxySet(methodName.substring(3), args);
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

        protected Object proxySet(String attrName, Object[] args)
        {
            assert args.length == 1;
            return put(attrName, args[0]);
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

        /**
         * Returns this object wrapped in a proxy. The proxy will implement the
         * interface specified in the {@link #clazz} member.
         */
        protected Object wrap()
        {
            return Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[] {clazz},
                this);
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
        RefPackageImpl(Class clazz)
        {
            super(clazz);
            // For each method which returns a class, create an attribute.
            Method[] methods = clazz.getMethods();
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
     * Abstract base class for an iterator over a JMI object and its children.
     */
    private static abstract class JmiVisitor {
        void accept(Object o) {
            // REVIEW: The current implementation isn't very elegant. It
            //   makes a lot of assumptions about how the JMI tree is
            //   represented. A 'real' visitor pattern -- with an 'accept'
            //   method on each handler object -- would be better.

            Class clazz = o.getClass();
            List attrNames = new ArrayList();
            List attrValues = new ArrayList();
            List collectionNames = new ArrayList();
            List collectionValues = new ArrayList();
            List refNames = new ArrayList();
            List refValues = new ArrayList();
            Method[] methods = clazz.getMethods();
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
            visit(o, attrNames, attrValues, collectionNames, collectionValues,
                refNames, refValues);
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
    private static class JmiPrinter extends JmiVisitor {
        // ~ Constants
        private static final Pattern poundIntColonPattern =
            Pattern.compile("#[0-9]*:");

        // ~ Data members
        private final XMLOutput xmlOutput;
        private final Map printIds = new HashMap();
        private int printId = 0;

        JmiPrinter(PrintWriter pw) {
            xmlOutput = new XMLOutput(pw);
            xmlOutput.setGlob(true);
        }

        void accept(Object o)
        {
            // Assign every object a synthetic id which is unique only during
            // this visitation. After the first visit, just print a reference.
            String id = (String) printIds.get(o);
            if (id == null) {
                // First visit.
                id = Integer.toString(printId++);
                printIds.put(o, id);
                super.accept(o);
            } else {
                // Second or subsequent visit.
                String tagName = getTagName(o);
                xmlOutput.beginBeginTag(tagName);
                xmlOutput.attribute("refid", id);
                xmlOutput.endBeginTag(tagName);
                xmlOutput.endTag(tagName);
            }
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
            // Print the synthetic id which is generated during printing.
            String id = (String) printIds.get(o);
            xmlOutput.attribute("id", id);
            for (int i = 0; i < attrNames.size(); i++) {
                String attrName = (String) attrNames.get(i);
                Object attrValue = attrValues.get(i);
                if (attrName.equals("name")) {
                    // Convert
                    //   name=\"MockTableImplRel#274:536\"
                    // into
                    //   name=\"MockTableImplRel#274:536\"
                    attrValue = poundIntColonPattern
                        .matcher((String) attrValue).replaceAll("#xxxx:");
                } else if (attrName.equals("streamId")) {
                    // Convert
                    //   streamId=\"536\"
                    // into
                    //   streamId=\"xxxx\"
                    attrValue = "xxxx";
                }
                xmlOutput.attribute(attrName, String.valueOf(attrValue));
            }
            xmlOutput.endBeginTag(tagName);
            for (int i = 0; i < refNames.size(); i++) {
                String refName = (String) refNames.get(i);
                RefBaseObject ref = (RefBaseObject) refValues.get(i);
                if (ref == null) {
                    continue;
                }
                xmlOutput.beginTag(refName, null);
                accept(ref);
                xmlOutput.endTag(refName);
            }
            for (int i = 0; i < collectionNames.size(); i++) {
                String collectionName = (String) collectionNames.get(i);
                xmlOutput.beginTag(collectionName, null);
                List list = (List) collectionValues.get(i);
                for (int j = 0; j < list.size(); j++) {
                    RefBaseObject ref = (RefBaseObject) list.get(j);
                    accept(ref);
                }
                xmlOutput.endTag(collectionName);
            }
            xmlOutput.endTag(tagName);
        }

        private String getTagName(Object o)
        {
            String className = o.getClass().getInterfaces()[0].getName();
            int dot = className.lastIndexOf('.');
            String tagName = (dot >= 0) ?
                className.substring(dot + 1) :
                className;
            return tagName;
        }
    }

}

// End MockMetadataFactory.java
