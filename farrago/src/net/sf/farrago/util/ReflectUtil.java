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
     * Use reflection to find the correct java.nio.ByteBuffer "absolute get"
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
     * Use reflection to find the correct java.nio.ByteBuffer "absolute put"
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
     * Get the Java boxing class for a primitive class.
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
     * Get the name of a class with no package qualifiers; if it's an
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
}


// End ReflectUtil.java
