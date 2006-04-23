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

package org.eigenbase.util;

import java.lang.reflect.*;
import java.nio.*;
import java.util.*;


/**
 * Static utilities for Java reflection.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class ReflectUtil
{
    //~ Static fields/initializers --------------------------------------------

    private static Map primitiveToBoxingMap;
    private static Map primitiveToByteBufferReadMethod;
    private static Map primitiveToByteBufferWriteMethod;

    static {
        primitiveToBoxingMap = new HashMap();
        primitiveToBoxingMap.put(Boolean.TYPE, Boolean.class);
        primitiveToBoxingMap.put(Byte.TYPE, Byte.class);
        primitiveToBoxingMap.put(Character.TYPE, Character.class);
        primitiveToBoxingMap.put(Double.TYPE, Double.class);
        primitiveToBoxingMap.put(Float.TYPE, Float.class);
        primitiveToBoxingMap.put(Integer.TYPE, Integer.class);
        primitiveToBoxingMap.put(Long.TYPE, Long.class);
        primitiveToBoxingMap.put(Short.TYPE, Short.class);

        primitiveToByteBufferReadMethod = new HashMap();
        primitiveToByteBufferWriteMethod = new HashMap();
        Method [] methods = ByteBuffer.class.getDeclaredMethods();
        for (int i = 0; i < methods.length; ++i) {
            Method method = methods[i];
            Class [] paramTypes = method.getParameterTypes();
            if (method.getName().startsWith("get")) {
                if (!method.getReturnType().isPrimitive()) {
                    continue;
                }
                if (paramTypes.length != 1) {
                    continue;
                }
                primitiveToByteBufferReadMethod.put(
                    method.getReturnType(),
                    method);

                // special case for Boolean:  treat as byte
                if (method.getReturnType().equals(Byte.TYPE)) {
                    primitiveToByteBufferReadMethod.put(Boolean.TYPE, method);
                }
            } else if (method.getName().startsWith("put")) {
                if (paramTypes.length != 2) {
                    continue;
                }
                if (!paramTypes[1].isPrimitive()) {
                    continue;
                }
                primitiveToByteBufferWriteMethod.put(paramTypes[1], method);

                // special case for Boolean:  treat as byte
                if (paramTypes[1].equals(Byte.TYPE)) {
                    primitiveToByteBufferWriteMethod.put(Boolean.TYPE, method);
                }
            }
        }
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Uses reflection to find the correct java.nio.ByteBuffer "absolute get"
     * method for a given primitive type.
     *
     * @param clazz the Class object representing the primitive type
     *
     * @return corresponding method
     */
    public static Method getByteBufferReadMethod(Class clazz)
    {
        assert (clazz.isPrimitive());
        return (Method) primitiveToByteBufferReadMethod.get(clazz);
    }

    /**
     * Uses reflection to find the correct java.nio.ByteBuffer "absolute put"
     * method for a given primitive type.
     *
     * @param clazz the Class object representing the primitive type
     *
     * @return corresponding method
     */
    public static Method getByteBufferWriteMethod(Class clazz)
    {
        assert (clazz.isPrimitive());
        return (Method) primitiveToByteBufferWriteMethod.get(clazz);
    }

    /**
     * Gets the Java boxing class for a primitive class.
     *
     * @param primitiveClass representative class for primitive
     * (e.g. java.lang.Integer.TYPE)
     *
     * @return corresponding boxing Class (e.g. java.lang.Integer)
     */
    public static Class getBoxingClass(Class primitiveClass)
    {
        assert (primitiveClass.isPrimitive());
        return (Class) primitiveToBoxingMap.get(primitiveClass);
    }

    /**
     * Gets the name of a class with no package qualifiers; if it's an
     * inner class, it will still be qualified by the containing class (X$Y).
     *
     * @param c the class of interest
     * @return the unqualified name
     */
    public static String getUnqualifiedClassName(Class c)
    {
        String className = c.getName();
        int lastDot = className.lastIndexOf('.');
        if (lastDot < 0) {
            return className;
        }
        return className.substring(lastDot + 1);
    }

    /**
     * Composes a string representing a human-readable method name
     * (with neither exception nor return type information).
     *
     * @param declaringClass class on which method is defined
     *
     * @param methodName simple name of method without signature
     *
     * @param paramTypes method parameter types
     *
     * @return unmangled method name
     */
    public static String getUnmangledMethodName(
        Class declaringClass,
        String methodName,
        Class [] paramTypes)
    {
        StringBuffer sb = new StringBuffer();
        sb.append(declaringClass.getName());
        sb.append(".");
        sb.append(methodName);
        sb.append("(");
        for (int i = 0; i < paramTypes.length; ++i) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(paramTypes[i].getName());
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Composes a string representing a human-readable method name
     * (with neither exception nor return type information).
     *
     * @param method method whose name is to be generated
     *
     * @return unmangled method name
     */
    public static String getUnmangledMethodName(
        Method method)
    {
        return getUnmangledMethodName(
            method.getDeclaringClass(),
            method.getName(),
            method.getParameterTypes());
    }

    /**
     * Implements the {@link Glossary#VisitorPattern} via reflection.  The
     * basic technique is taken from <a
     * href="http://www.javaworld.com/javaworld/javatips/jw-javatip98.html"> a
     * Javaworld article</a>.  For an example of how to use it, see {@link
     * ReflectVisitorTest}.  Visit method lookup follows the same rules as if
     * compile-time resolution for VisitorClass.visit(VisiteeClass) were
     * performed.  An ambiguous match due to multiple interface inheritance
     * results in an IllegalArgumentException.  A non-match is indicated by
     * returning false.
     *
     * @param visitor object whose visit method is to be invoked
     *
     * @param visitee object to be passed as a parameter to the visit method
     *
     * @param hierarchyRoot if non-null, visitor method will only
     * be invoked if it takes a parameter whose type is a subtype of
     * hierarchyRoot
     *
     * @param visitMethodName name of visit method, e.g. "visit"
     *
     * @return true if a matching visit method was found and invoked
     */
    public static boolean invokeVisitor(
        Object visitor,
        Object visitee,
        Class hierarchyRoot,
        String visitMethodName)
    {
        Class visitorClass = visitor.getClass();
        Class visiteeClass = visitee.getClass();
        Method method = lookupVisitMethod(
            visitorClass, visiteeClass, visitMethodName);
        if (method == null) {
            return false;
        }

        if (hierarchyRoot != null) {
            Class paramType = method.getParameterTypes()[0];
            if (!hierarchyRoot.isAssignableFrom(paramType)) {
                return false;
            }
        }
        
        try {
            method.invoke(visitor, new Object[] { visitee });
        } catch (IllegalAccessException ex) {
            throw Util.newInternal(ex);
        } catch (InvocationTargetException ex) {
            // visit methods aren't allowed to have throws clauses,
            // so the only exceptions which should come
            // to us are RuntimeExceptions and Errors
            Throwable t = ex.getTargetException();
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else if (t instanceof Error) {
                throw (Error) t;
            } else {
                throw new AssertionError(t.getClass().getName());
            }
        }
        return true;
    }

    /**
     * Looks up a visit method.
     *
     * @param visitorClass class of object whose visit method is to be invoked
     *
     * @param visiteeClass class of object to be passed as a parameter to the
     * visit method
     *
     * @param visitMethodName name of visit method
     *
     * @return method found, or null if none found
     */
    public static Method lookupVisitMethod(
        Class visitorClass,
        Class visiteeClass,
        String visitMethodName)
    {
        return lookupVisitMethod(
            visitorClass, visiteeClass, visitMethodName,
            Collections.EMPTY_LIST);
    }
    
    /**
     * Looks up a visit method taking additional parameters beyond
     * the overloaded visitee type.
     *
     * @param visitorClass class of object whose visit method is to be invoked
     *
     * @param visiteeClass class of object to be passed as a parameter to the
     * visit method
     *
     * @param visitMethodName name of visit method
     *
     * @param additionalParameterTypes list of additional parameter types
     *
     * @return method found, or null if none found
     */
    public static Method lookupVisitMethod(
        Class visitorClass,
        Class visiteeClass,
        String visitMethodName,
        List<Class> additionalParameterTypes)
    {
        // TODO jvs 28-Nov-2004:  cache results in a dispatch map

        Class [] paramTypes = new Class[1 + additionalParameterTypes.size()];
        int iParam = 0;
        paramTypes[iParam++] = visiteeClass;
        for (Class paramType : additionalParameterTypes) {
            paramTypes[iParam++] = paramType;
        }
    
        try {
            return visitorClass.getMethod(
                visitMethodName, paramTypes);
        } catch (NoSuchMethodException ex) {
            // not found:  carry on with lookup
        }

        Method candidateMethod = null;

        Class superClass = visiteeClass.getSuperclass();
        if (superClass != null) {
            candidateMethod = lookupVisitMethod(
                visitorClass, superClass, visitMethodName,
                additionalParameterTypes);
        }

        Class [] interfaces = visiteeClass.getInterfaces();
        for (int i = 0; i < interfaces.length; ++i) {
            Method method = lookupVisitMethod(
                visitorClass, interfaces[i], visitMethodName,
                additionalParameterTypes);
            if (method != null) {
                if (candidateMethod != null) {
                    if (!method.equals(candidateMethod)) {
                        Class c1 = method.getParameterTypes()[0];
                        Class c2 = candidateMethod.getParameterTypes()[0];
                        if (c1.isAssignableFrom(c2)) {
                            // c2 inherits from c1, so keep candidateMethod
                            continue;
                        } else if (c2.isAssignableFrom(c1)) {
                            // c1 inherits from c2, so fall through
                            // to set candidateMethod = method
                        } else {
                            // c1 and c2 are not directly related
                            throw new IllegalArgumentException(
                                "dispatch ambiguity between "
                                + candidateMethod
                                + " and "
                                + method);
                        }
                    }
                }
                candidateMethod = method;
            }
        }

        return candidateMethod;
    }

    /**
     * Looks up a class by name.  This is like Class.forName,
     * except that it handles primitive type names.
     *
     * @param name fully-qualified name of class to look up
     *
     * @return class
     */
    public static Class getClassForName(String name)
        throws Exception
    {
        if (name.equals("boolean")) {
            return boolean.class;
        } else if (name.equals("byte")) {
            return byte.class;
        } else if (name.equals("char")) {
            return char.class;
        } else if (name.equals("double")) {
            return double.class;
        } else if (name.equals("float")) {
            return float.class;
        } else if (name.equals("int")) {
            return int.class;
        } else if (name.equals("long")) {
            return long.class;
        } else if (name.equals("short")) {
            return short.class;
        } else if (name.equals("void")) {
            return void.class;
        } else {
            return Class.forName(name);
        }
    }
}


// End ReflectUtil.java
