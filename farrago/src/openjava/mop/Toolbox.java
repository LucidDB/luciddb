/*
 * $Id$
 *
 * comments here.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1999 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.mop;


import openjava.ptree.*;
import openjava.ptree.util.ParseTreeVisitor;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptConnection;
import org.eigenbase.runtime.SyntheticObject;
import org.eigenbase.util.Util;

import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.util.*;

/**
 * The class <code>Toolbox</code> is a utility class.
 * <p>
 * <pre>
 * </pre>
 * <p>
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public abstract class Toolbox {
    public static final OJClass clazzVoid = OJClass.forClass(
            void.class);

    public static final OJClass clazzObject = OJClass.forClass(
            java.lang.Object.class);

    public static final OJClass clazzObjectArray = OJClass.arrayOf(
            clazzObject);

    public static final OJClass clazzConnection = OJClass.forClass(
            RelOptConnection.class);

    public static final OJClass clazzCollection = OJClass.forClass(
            java.util.Collection.class);

    public static final OJClass clazzMap = OJClass.forClass(
            java.util.Map.class);

    public static final OJClass clazzMapEntry = OJClass.forClass(
            java.util.Map.Entry.class);

    public static final OJClass clazzHashtable = OJClass.forClass(
            java.util.Hashtable.class);

    public static final OJClass clazzEnumeration = OJClass.forClass(
            java.util.Enumeration.class);

    public static final OJClass clazzIterator = OJClass.forClass(
            java.util.Iterator.class);

    public static final OJClass clazzIterable = OJClass.forClass(
            org.eigenbase.runtime.Iterable.class);

    public static final OJClass clazzVector = OJClass.forClass(
            java.util.Vector.class);

    public static final OJClass clazzComparable = OJClass.forClass(
            java.lang.Comparable.class);

    public static final OJClass clazzComparator = OJClass.forClass(
            java.util.Comparator.class);

    public static final OJClass clazzResultSet = OJClass.forClass(
            java.sql.ResultSet.class);

    /*
    public static final OJClass clazzAggregationExtender = OJClass.forClass(
            AggregationExtender.class);

    public static final OJClass clazzAggAndAcc = OJClass.forClass(
            AggAndAcc.class);
    */

    public static final OJClass clazzSyntheticObject = OJClass.forClass(
            SyntheticObject.class);

    public static final OJClass clazzClass = OJClass.forClass(
            java.lang.Class.class);

    public static final OJClass clazzString = OJClass.forClass(
            java.lang.String.class);

    /*
    public static final OJClass clazzSaffronUtil = OJClass.forClass(
            org.eigenbase.runtime.SaffronUtil.class);
    */

    public static final OJClass clazzSet = OJClass.forClass(
            java.util.Set.class);

    public static final OJClass clazzSQLException = OJClass.forClass(
            java.sql.SQLException.class);

    public static final OJClass clazzEntry = OJClass.forClass(
            java.util.Map.Entry.class);

    public static final OJClass[] emptyArrayOfOJClass = new OJClass[]{};

    /**
     * Generates an array of classes containing the declared classes and
     * the based classes except the declared one.
     *
     * @param  declareds  declared classes to override
     * @param  bases  based classes.
     * @return  classes which contains the declared classes and the based
     *      classes except the declared one.
     * @see openjava.mop.OJClass
     */
    public static final OJClass[]
            overridesOn(OJClass[] declareds, OJClass[] bases) {
        Hashtable table = new Hashtable();
        for (int i = 0; i < bases.length; ++i) {
            table.put(bases[i].signature(), bases[i]);
        }
        for (int i = 0; i < declareds.length; ++i) {
            table.put(declareds[i].signature(), declareds[i]);
        }

        OJClass[] result = new OJClass[table.size()];
        Enumeration it = table.elements();
        for (int i = 0; it.hasMoreElements(); ++i) {
            result[i] = (OJClass) it.nextElement();
        }

        return result;
    }

    /**
     * Generates an array of fields containing the declared fields and
     * the based fields except the declared one.
     *
     * @param  declareds  declared fields to override
     * @param  bases  based fields.
     * @return  fields which contains the declared fields and the based
     *      fields except the declared one.
     * @see openjava.mop.OJField
     */
    public static final OJField[]
            overridesOn(OJField[] declareds, OJField[] bases) {
        Hashtable table = new Hashtable();
        for (int i = 0; i < bases.length; ++i) {
            table.put(bases[i].signature(), bases[i]);
        }
        for (int i = 0; i < declareds.length; ++i) {
            table.put(declareds[i].signature(), declareds[i]);
        }

        OJField[] result = new OJField[table.size()];
        Enumeration it = table.elements();
        for (int i = 0; it.hasMoreElements(); ++i) {
            result[i] = (OJField) it.nextElement();
        }

        return result;
    }

    /**
     * Generates an array of methods containing the declared methods and
     * the based methods except the declared one.
     *
     * @param  declareds  declared methods to override
     * @param  bases  based methods.
     * @return  methods which contains the declared methods and the based
     *      methods except the declared one.
     * @see openjava.mop.OJMethod
     */
    public static final OJMethod[]
            overridesOn(OJMethod[] declareds, OJMethod[] bases) {
        Hashtable table = new Hashtable();
        for (int i = 0; i < bases.length; ++i) {
            table.put(bases[i].signature(), bases[i]);
        }
        for (int i = 0; i < declareds.length; ++i) {
            table.put(declareds[i].signature(), declareds[i]);
        }

        OJMethod[] result = new OJMethod[table.size()];
        Enumeration it = table.elements();
        for (int i = 0; it.hasMoreElements(); ++i) {
            result[i] = (OJMethod) it.nextElement();
        }

        return result;
    }

    /**
     * Generates an array of classes containing the source classes
     * except ones with private access modifier.
     *
     * @param  src_classes  source classes.
     * @return  classes except ones with private access modifier.
     * @see openjava.mop.OJModifier
     */
    public static final OJClass[]
            removeThePrivates(OJClass[] src_classes) {
        int dest_length = 0;
        for (int i = 0; i < src_classes.length; ++i) {
            OJModifier modif = src_classes[i].getModifiers();
            if (!modif.isPrivate()) dest_length++;
        }

        OJClass[] result = new OJClass[dest_length];
        for (int i = 0, count = 0; i < src_classes.length; ++i) {
            OJModifier modif = src_classes[i].getModifiers();
            if (!modif.isPrivate()) result[count++] = src_classes[i];
        }

        return result;
    }

    /**
     * Generates an array of fields containing the source fields
     * except ones with private access modifier.
     *
     * @param  src_fields  source fields.
     * @return  fields except ones with private access modifier.
     * @see openjava.mop.OJModifier
     */
    public static final OJField[]
            removeThePrivates(OJField[] src_fields) {
        int dest_length = 0;
        for (int i = 0; i < src_fields.length; ++i) {
            OJModifier modif = src_fields[i].getModifiers();
            if (!modif.isPrivate()) dest_length++;
        }

        OJField[] result = new OJField[dest_length];
        for (int i = 0, count = 0; i < src_fields.length; ++i) {
            OJModifier modif = src_fields[i].getModifiers();
            if (!modif.isPrivate()) result[count++] = src_fields[i];
        }

        return result;
    }

    /**
     * Generates an array of methods containing the source methods
     * except ones with private access modifier.
     *
     * @param  src_methods  source methods.
     * @return  methods except ones with private access modifier.
     * @see openjava.mop.OJModifier
     */
    public static final OJMethod[]
            removeThePrivates(OJMethod[] src_methods) {
        int dest_length = 0;
        for (int i = 0; i < src_methods.length; ++i) {
            OJModifier modif = src_methods[i].getModifiers();
            if (!modif.isPrivate()) dest_length++;
        }

        OJMethod[] result = new OJMethod[dest_length];
        for (int i = 0, count = 0; i < src_methods.length; ++i) {
            OJModifier modif = src_methods[i].getModifiers();
            if (!modif.isPrivate()) result[count++] = src_methods[i];
        }

        return result;
    }

    /**
     * Generates an array of constructors containing the source
     * constructors except ones with private access modifier.
     *
     * @param  src_constrs  source constructors.
     * @return  constructors except ones with private access modifier.
     * @see openjava.mop.OJModifier
     */
    public static final OJConstructor[]
            removeThePrivates(OJConstructor[] src_constrs) {
        int dest_length = 0;
        for (int i = 0; i < src_constrs.length; ++i) {
            OJModifier modif = src_constrs[i].getModifiers();
            if (!modif.isPrivate()) dest_length++;
        }

        OJConstructor[] result = new OJConstructor[dest_length];
        for (int i = 0, count = 0; i < src_constrs.length; ++i) {
            OJModifier modif = src_constrs[i].getModifiers();
            if (!modif.isPrivate()) result[count++] = src_constrs[i];
        }

        return result;
    }

    /**
     * Generates an array of classes containing the source classes
     * except ones with private access modifier.
     *
     * @param  src_classes  source classes.
     * @return  classes except ones with private access modifier.
     * @see openjava.mop.OJModifier
     */
    public static final OJClass[]
            removeTheDefaults(OJClass[] src_classes) {
        int dest_length = 0;
        for (int i = 0; i < src_classes.length; ++i) {
            OJModifier modif = src_classes[i].getModifiers();
            if (modif.isPrivate() || modif.isProtected() || modif.isPublic()) {
                dest_length++;
            }
        }

        OJClass[] result = new OJClass[dest_length];
        for (int i = 0, count = 0; i < src_classes.length; ++i) {
            OJModifier modif = src_classes[i].getModifiers();
            if (modif.isPrivate() || modif.isProtected() || modif.isPublic()) {
                result[count++] = src_classes[i];
            }
        }

        return result;
    }

    /**
     * Generates an array of fields containing the source fields
     * except ones with private access modifier.
     *
     * @param  src_fields  source fields.
     * @return  fields except ones with private access modifier.
     * @see openjava.mop.OJModifier
     */
    public static final OJField[]
            removeTheDefaults(OJField[] src_fields) {
        int dest_length = 0;
        for (int i = 0; i < src_fields.length; ++i) {
            OJModifier modif = src_fields[i].getModifiers();
            if (modif.isPrivate() || modif.isProtected() || modif.isPublic()) {
                dest_length++;
            }
        }

        OJField[] result = new OJField[dest_length];
        for (int i = 0, count = 0; i < src_fields.length; ++i) {
            OJModifier modif = src_fields[i].getModifiers();
            if (modif.isPrivate() || modif.isProtected() || modif.isPublic()) {
                result[count++] = src_fields[i];
            }
        }

        return result;
    }

    /**
     * Generates an array of methods containing the source methods
     * except ones with private access modifier.
     *
     * @param  src_methods  source methods.
     * @return  methods except ones with private access modifier.
     * @see openjava.mop.OJModifier
     */
    public static final OJMethod[]
            removeTheDefaults(OJMethod[] src_methods) {
        int dest_length = 0;
        for (int i = 0; i < src_methods.length; ++i) {
            OJModifier modif = src_methods[i].getModifiers();
            if (modif.isPrivate() || modif.isProtected() || modif.isPublic()) {
                dest_length++;
            }
        }

        OJMethod[] result = new OJMethod[dest_length];
        for (int i = 0, count = 0; i < src_methods.length; ++i) {
            OJModifier modif = src_methods[i].getModifiers();
            if (modif.isPrivate() || modif.isProtected() || modif.isPublic()) {
                result[count++] = src_methods[i];
            }
        }

        return result;
    }

    /**
     * Generates an array of constructors containing the source
     * constructors except ones with private access modifier.
     *
     * @param  src_constrs  source constructors.
     * @return  constructors except ones with private access modifier.
     * @see openjava.mop.OJModifier
     */
    public static final OJConstructor[]
            removeTheDefaults(OJConstructor[] src_constrs) {
        int dest_length = 0;
        for (int i = 0; i < src_constrs.length; ++i) {
            OJModifier modif = src_constrs[i].getModifiers();
            if (modif.isPrivate() || modif.isProtected() || modif.isPublic()) {
                dest_length++;
            }
        }

        OJConstructor[] result = new OJConstructor[dest_length];
        for (int i = 0, count = 0; i < src_constrs.length; ++i) {
            OJModifier modif = src_constrs[i].getModifiers();
            if (modif.isPrivate() || modif.isProtected() || modif.isPublic()) {
                result[count++] = src_constrs[i];
            }
        }

        return result;
    }

    /**
     * Generates an array of classes containing the source classes
     * except ones with non-public access modifier;
     * one of private, protected or package level access modifiers.
     *
     * @param  src_classes  source classes.
     * @return  classes except ones with non-public access modifier.
     * @see openjava.mop.OJModifier
     */
    public static final OJClass[]
            removeTheNonPublics(OJClass[] src_classes) {
        int dest_length = 0;
        for (int i = 0; i < src_classes.length; ++i) {
            OJModifier modif = src_classes[i].getModifiers();
            if (modif.isPublic()) dest_length++;
        }

        OJClass[] result = new OJClass[dest_length];
        for (int i = 0, count = 0; i < src_classes.length; ++i) {
            OJModifier modif = src_classes[i].getModifiers();
            if (modif.isPublic()) result[count++] = src_classes[i];
        }

        return result;
    }

    /**
     * Generates an array of fields containing the source fields
     * except ones with non-public access modifier;
     * one of private, protected or package level access modifiers.
     *
     * @param  src_fields  source fields.
     * @return  fields except ones with non-public access modifier.
     * @see openjava.mop.OJModifier
     */
    public static final OJField[]
            removeTheNonPublics(OJField[] src_fields) {
        int dest_length = 0;
        for (int i = 0; i < src_fields.length; ++i) {
            OJModifier modif = src_fields[i].getModifiers();
            if (modif.isPublic()) dest_length++;
        }

        OJField[] result = new OJField[dest_length];
        for (int i = 0, count = 0; i < src_fields.length; ++i) {
            OJModifier modif = src_fields[i].getModifiers();
            if (modif.isPublic()) result[count++] = src_fields[i];
        }

        return result;
    }

    /**
     * Generates an array of methods containing the source methods
     * except ones with non-public access modifier;
     * one of private, protected or package level access modifiers.
     *
     * @param  src_methods  source methods.
     * @return  methods except ones with non-public access modifier.
     * @see openjava.mop.OJModifier
     */
    public static final OJMethod[]
            removeTheNonPublics(OJMethod[] src_methods) {
        int dest_length = 0;
        for (int i = 0; i < src_methods.length; ++i) {
            OJModifier modif = src_methods[i].getModifiers();
            if (modif.isPublic()) dest_length++;
        }

        OJMethod[] result = new OJMethod[dest_length];
        for (int i = 0, count = 0; i < src_methods.length; ++i) {
            OJModifier modif = src_methods[i].getModifiers();
            if (modif.isPublic()) result[count++] = src_methods[i];
        }

        return result;
    }

    /**
     * Generates an array of constructors containing the source constructors
     * except ones with non-public access modifier;
     * one of private, protected or package level access modifiers.
     *
     * @param  src_constrs  source constructors.
     * @return  constructors except ones with non-public access modifier.
     * @see openjava.mop.OJModifier
     */
    public static final OJConstructor[]
            removeTheNonPublics(OJConstructor[] src_constrs) {
        int dest_length = 0;
        for (int i = 0; i < src_constrs.length; ++i) {
            OJModifier modif = src_constrs[i].getModifiers();
            if (modif.isPublic()) dest_length++;
        }

        OJConstructor[] result = new OJConstructor[dest_length];
        for (int i = 0, count = 0; i < src_constrs.length; ++i) {
            OJModifier modif = src_constrs[i].getModifiers();
            if (modif.isPublic()) result[count++] = src_constrs[i];
        }

        return result;
    }

    /**
     * Pick up a field with the specified name in the source array
     * of fields.
     *
     * @param  src_fields source fields.
     * @param  name  a name to specify.
     * @return  a field with the specified name.
     * @see openjava.mop.OJClass
     */
    public static final OJField
            pickupField(OJField[] src_fields, String name) {
        for (int i = 0; i < src_fields.length; ++i) {
            if (name.equals(src_fields[i].getName())) {
                return src_fields[i];
            }
        }
        return null;
    }

    /**
     * Pick up a method with the specified signature in the source
     * array of methods.
     *
     * @param  src_methods  source methods.
     * @param  name  a name to specify.
     * @param  param_types  parameter types to specify.
     * @return  a method with the specified signature.
     *      This returns null if it doesn't exist.
     * @see openjava.mop.OJClass
     */
    public static final OJMethod
            pickupMethod(OJMethod[] src_methods, String name,
                         OJClass[] param_types) {
        src_methods = pickupMethodsByName(src_methods, name);
        return pickupMethodByParameterTypes(src_methods, param_types);
    }

    /**
     * Pick up a method with the signature acceptable the specified
     * signature in the source array of methods.
     *
     * @param  src_methods  source methods.
     * @param  name  a name to specify.
     * @param  param_types  parameter types to specify.
     * @return  a method with the specified signature.
     *      This returns null if it doesn't exist.
     * @see openjava.mop.OJClass
     */
    public static final OJMethod
            pickupAcceptableMethod(OJMethod[] src_methods, String name,
                                   OJClass[] param_types) {
        src_methods
                = pickupAcceptableMethods(src_methods, name, param_types);
        return pickupMostSpecified(src_methods);
    }

    /**
     * Generates an array of methods containing the methods with the
     * signature acceptable the specified signature in the source
     * array of methods.
     *
     * @param  src_methods  source methods.
     * @param  name  a name to specify.
     * @param  param_types  parameter types to specify.
     * @return  methods with the specified signature.
     * @see openjava.mop.OJClass
     */
    public static final OJMethod[]
            pickupAcceptableMethods(OJMethod[] src_methods, String name,
                                    OJClass[] param_types) {
        src_methods = pickupMethodsByName(src_methods, name);
        return pickupAcceptableMethodsByParameterTypes(src_methods,
                param_types);
    }

    /**
     * Pick up a constructor with the specified signature in the source
     * array of constructors.
     *
     * @param  src_constrs  source constructors.
     * @param  param_types  parameter types to specify.
     * @return  a Constructor with the specified signature.
     *      This returns null if it doesn't exist.
     * @see openjava.mop.OJClass
     */
    public static final OJConstructor
            pickupConstructor(OJConstructor[] src_constrs, OJClass[] param_types) {
        if (param_types == null) param_types = new OJClass[0];
        for (int i = 0; i < src_constrs.length; ++i) {
            OJClass[] accepter = src_constrs[i].getParameterTypes();
            if (isSame(accepter, param_types)) return src_constrs[i];
        }
        return null;
    }

    /**
     * Pick up a constructor with the signature acceptable the specified
     * signature in the source array of constructors.
     *
     * @param  src_constrs  source constructors.
     * @param  param_types  parameter types to specify.
     * @return  a constructor with the specified signature.
     *      This returns null if it doesn't exist.
     * @see openjava.mop.OJClass
     */
    public static final OJConstructor
            pickupAcceptableConstructor(OJConstructor[] src_constrs,
                                        OJClass[] param_types) {
        src_constrs = pickupAcceptableConstructors(src_constrs, param_types);
        return pickupMostSpecified(src_constrs);
    }

    /**
     * Generates an array of constructors containing the constructors
     * with the specified parameter types in the source array of
     * constructors.
     *
     * @param  src_constrs  source constructors.
     * @param  param_types  parameter types to specify.
     * @return  constructors acceptable the specified parameter types.
     * @see openjava.mop.OJClass
     */
    public static final OJConstructor[]
            pickupAcceptableConstructors(OJConstructor[] src_constrs,
                                         OJClass[] param_types) {
        param_types = (param_types == null) ? new OJClass[0] : param_types;
        int dest_length = 0;
        for (int i = 0; i < src_constrs.length; ++i) {
            OJClass[] accepter = src_constrs[i].getParameterTypes();
            if (isAcceptable(accepter, param_types)) dest_length++;
        }

        OJConstructor[] result = new OJConstructor[dest_length];
        for (int i = 0, count = 0; i < src_constrs.length; ++i) {
            OJClass[] accepter = src_constrs[i].getParameterTypes();
            if (isAcceptable(accepter, param_types)) {
                result[count++] = src_constrs[i];
            }
        }

        return result;
    }

    /**
     * Generates an array of methods containing the methods with the
     * specified name in the source array of methods.
     *
     * @param  src_methods  source methods.
     * @param  name  a name to specify.
     * @return  methods with the specified name.
     * @see openjava.mop.OJClass
     */
    public static final OJMethod[]
            pickupMethodsByName(OJMethod[] src_methods, String name) {
        int dest_length = 0;
        for (int i = 0; i < src_methods.length; ++i) {
            if (name.equals(src_methods[i].getName())) dest_length++;
        }

        OJMethod[] result = new OJMethod[dest_length];
        for (int i = 0, count = 0; i < src_methods.length; ++i) {
            if (name.equals(src_methods[i].getName())) {
                result[count++] = src_methods[i];
            }
        }

        return result;
    }

    /**
     * Picks up a method with the specified parameter types in the source
     * array of methods.
     *
     * @param  src_methods  source methods.
     * @param  param_types  parameter types to specify.
     * @return  a method with the specified parameter types.
     * @see openjava.mop.OJClass
     */
    public static final OJMethod
            pickupMethodByParameterTypes(OJMethod[] src_methods,
                                         OJClass[] param_types) {
        if (param_types == null) param_types = new OJClass[0];
        for (int i = 0; i < src_methods.length; ++i) {
            OJClass[] accepter = src_methods[i].getParameterTypes();
            if (isSame(accepter, param_types)) return src_methods[i];
        }
        return null;
    }

    /**
     * Generates an array of methods containing the methods with the
     * parameter types acceptable specified parameter types in the source
     * array of methods.
     *
     * @param  src_methods  source methods.
     * @param  param_types  parameter types to specify.
     * @return  methods acceptable the specified parameter types.
     * @see openjava.mop.OJClass
     */
    public static final OJMethod[]
            pickupAcceptableMethodsByParameterTypes(OJMethod[] src_methods,
                                                    OJClass[] param_types) {
        if (param_types == null) param_types = new OJClass[0];
        int dest_length = 0;
        for (int i = 0; i < src_methods.length; ++i) {
            OJClass[] accepter = src_methods[i].getParameterTypes();
            if (isAcceptable(accepter, param_types)) dest_length++;
        }

        OJMethod[] result = new OJMethod[dest_length];
        for (int i = 0, count = 0; i < src_methods.length; ++i) {
            OJClass[] accepter = src_methods[i].getParameterTypes();
            if (isAcceptable(accepter, param_types)) {
                result[count++] = src_methods[i];
            }
        }

        return result;
    }

    public static final boolean
            isSame(OJClass[] accepter, OJClass[] acceptee) {
        if (accepter.length != acceptee.length) return false;
        for (int i = 0; i < acceptee.length; ++i) {
            if (accepter[i] != acceptee[i]) return false;
        }
        return true;
    }

    public static final boolean
            isAcceptable(OJClass[] accepter, OJClass[] acceptee) {
        if (accepter.length != acceptee.length) return false;
        for (int i = 0; i < acceptee.length; ++i) {
            if (!accepter[i].isAssignableFrom(acceptee[i])) return false;
        }
        return true;
    }

    public static final boolean
            isAdaptableTo(OJClass[] adapter, OJClass[] adaptee) {
        if (adapter.length != adaptee.length) return false;
        for (int i = 0; i < adaptee.length; ++i) {
            if (!adaptee[i].isAssignableFrom(adapter[i])) return false;
        }
        return true;
    }

    public static final OJConstructor
            pickupMostSpecified(OJConstructor[] constrs) {
        if (constrs.length == 0) return null;
        OJConstructor most = constrs[0];
        for (int i = 0; i < constrs.length; ++i) {
            OJClass[] adapter = most.getParameterTypes();
            OJClass[] adaptee = constrs[i].getParameterTypes();
            if (!isAdaptableTo(adapter, adaptee)) most = constrs[i];
        }
        return most;
    }

    public static final OJMethod
            pickupMostSpecified(OJMethod[] methods) {
        if (methods.length == 0) return null;
        OJMethod most = methods[0];
        for (int i = 0; i < methods.length; ++i) {
            OJClass[] adapter = most.getParameterTypes();
            OJClass[] adaptee = methods[i].getParameterTypes();
            if (!isAdaptableTo(adapter, adaptee)) most = methods[i];
        }
        return most;
    }

    public static final OJClass[] append(OJClass[] a, OJClass[] b) {
        OJClass[] result = new OJClass[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    public static final OJField[] append(OJField[] a, OJField[] b) {
        OJField[] result = new OJField[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    public static final OJMethod[] append(OJMethod[] a, OJMethod[] b) {
        OJMethod[] result = new OJMethod[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    public static final OJConstructor[]
            append(OJConstructor[] a, OJConstructor[] b) {
        OJConstructor[] result = new OJConstructor[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    public static final String nameForJavaClassName(String jcname) {
        if (!jcname.startsWith("[")) return jcname;

        String stripped = stripHeadBracket(jcname);
        String result;

        if (stripped.startsWith("[")) {
            /* array of array like "[[Ljava.lang.Object;" or "[[I" */
            result = (nameForJavaClassName(stripped) + "[]");
        } else if (stripped.endsWith(";")) {
            /* array of class type like "[Ljava.lang.Object;" */
            result = (stripped.substring(1, stripped.length() - 1) + "[]");
        } else {
            /* array of primitive type like "[I" */
            switch (stripped.charAt(stripped.length() - 1)) {
            case 'Z':
                return "boolean[]";
            case 'B':
                return "byte[]";
            case 'C':
                return "char[]";
            case 'D':
                return "double[]";
            case 'F':
                return "float[]";
            case 'I':
                return "int[]";
            case 'J':
                return "long[]";
            case 'S':
                return "short[]";
            default :
                return "<unknown primitive type>";
            }
        }
        return result.replace('$', '.');
    }

    public static final String nameToJavaClassName(String ojcname) {
        if (!ojcname.endsWith("[]")) return ojcname;

        String stripped = stripBrackets(ojcname);

        if (stripped.endsWith("[]")) {
            /* array of array like "java.lang.Object[][]" or "int[][]" */
            return ("[" + nameToJavaClassName(stripped));
        } else if (stripped.equals("boolean")) {
            return "[Z";
        } else if (stripped.equals("byte")) {
            return "[B";
        } else if (stripped.equals("char")) {
            return "[C";
        } else if (stripped.equals("double")) {
            return "[D";
        } else if (stripped.equals("float")) {
            return "[F";
        } else if (stripped.equals("int")) {
            return "[I";
        } else if (stripped.equals("long")) {
            return "[J";
        } else if (stripped.equals("short")) {
            return "[S";
        } else {
            return ("[L" + stripBrackets(ojcname) + ";");
        }
    }

    private static final String stripHeadBracket(String jcname) {
        return jcname.substring(1);
    }

    private static final String stripBrackets(String ojcname) {
        return ojcname.substring(0, ojcname.length() - 2);
    }

    public static final OJClass
            forNameAnyway(Environment env, String name) {
        name = name.replace('$','.');
        name = env.toQualifiedName(name);
        OJClass result = env.lookupClass(name);
        if (result != null) return result;
        try {
            return OJClass.forName(name);
        } catch (OJClassNotFoundException e) {
            throw Util.newInternal(
                e,
                "OJClass.forNameAnyway() failed for : " + name);
        }
    }

    public static final OJClass[]
            arrayForNames(Environment env, String[] names) {
        OJClass[] result = new OJClass[names.length];
        for (int i = 0; i < result.length; ++i) {
            result[i] = forNameAnyway(env, names[i]);
        }
        return result;
    }

    public static final TypeName[] TNsForOJClasses(OJClass[] classes) {
        TypeName[] result
                = new TypeName[(classes == null) ? 0 : classes.length];
        for (int i = 0; i < result.length; ++i) {
            result[i] = TypeName.forOJClass(classes[i]);
        }
        return result;
    }

    private static final String PARAMETER_NAME = "oj_param";

    public static final ParameterList
            generateParameters(OJClass[] parameterTypes) {
        ParameterList result = new ParameterList();
        if (parameterTypes == null) return result;
        for (int i = 0; i < parameterTypes.length; ++i) {
            TypeName type = TypeName.forOJClass(parameterTypes[i]);
            Parameter param = new Parameter(type, PARAMETER_NAME + i);
            result.add(param);
        }
        return result;
    }

    public static final ParameterList
            generateParameters(OJClass[] parameterTypes, String[] parameterNames) {
        ParameterList result = new ParameterList();
        if (parameterTypes == null) return result;
        for (int i = 0; i < parameterTypes.length; ++i) {
            TypeName type = TypeName.forOJClass(parameterTypes[i]);
            Parameter param = new Parameter(type, parameterNames[i]);
            result.add(param);
        }
        return result;
    }

    /**
     * Guesses the row-type of an expression which has type <code>clazz</code>.
     * For example, {@link String}[] --> {@link String}; {@link
     * java.util.Iterator} --> {@link Object}.
     */
    public static final OJClass guessRowType(OJClass clazz) {
        if (clazz.isArray()) {
            return clazz.getComponentType();
        } else if (clazzIterator.isAssignableFrom(clazz) ||
                clazzEnumeration.isAssignableFrom(clazz) ||
                clazzVector.isAssignableFrom(clazz) ||
                clazzCollection.isAssignableFrom(clazz) ||
                clazzResultSet.isAssignableFrom(clazz)) {
            return clazzObject;
        } else if (clazzHashtable.isAssignableFrom(clazz) ||
                clazzMap.isAssignableFrom(clazz)) {
            return clazzEntry;
        } else {
            return null;
        }
    }

    /**
     * Guesses the row-type of an expression which has type <code>clazz</code>.
     * For example, {@link String}[] --> {@link String};
     * {@link java.util.Iterator} --> {@link Object}.
     */
    public static final Class guessRowType(Class clazz) {
        if (clazz.isArray()) {
            return clazz.getComponentType();
        } else if (Iterator.class.isAssignableFrom(clazz) ||
                Enumeration.class.isAssignableFrom(clazz) ||
                Vector.class.isAssignableFrom(clazz) ||
                Collection.class.isAssignableFrom(clazz) ||
                ResultSet.class.isAssignableFrom(clazz)) {
            return Object.class;
        } else if (Hashtable.class.isAssignableFrom(clazz) ||
                Map.class.isAssignableFrom(clazz)) {
            throw new UnsupportedOperationException(); // todo:
        } else {
            return null;
        }
    }

    public static OJClass getType(Environment env, Expression exp) {
        try {
            OJClass clazz = exp.getType(env);
            assert(clazz != null);
            return clazz;
        } catch (Exception e) {
            throw Util.newInternal(e, "while deriving type for '" + exp + "'");
        }
    }

    public static OJClass[] getTypes(Environment env, Expression[] exps) {
        OJClass[] classes = new OJClass[exps.length];
        for (int i = 0; i < classes.length; i++) {
            classes[i] = getType(env, exps[i]);
        }
        return classes;
    }

    public static OJClass getRowType(Environment env, Expression exp) {
        try {
            return exp.getRowType(env);
        } catch (Exception e) {
            System.err.println(
                    "Toolbox.getRowType() failed computing row type of " + exp +
                    ": " + e);
            return null;
        }
    }

    /**
     * Finds the type of a parse tree node (which may be an {@link Expression}
     * or a {@link TypeName}).
     *
     * @pre ref instanceof Expression || ref instanceof TypeName
     * @post return != null
     */
    public static OJClass getType(Environment env, ParseTree ref) {
        if (ref instanceof TypeName) {
            TypeName refType = (TypeName) ref;
            String qname = env.toQualifiedName(refType.toString());
            OJClass clazz = env.lookupClass(qname);
            if (clazz == null) {
                throw Util.newInternal(
                        "unknown type '" + refType + "'");
            }
            return clazz;
        } else if (ref instanceof Expression) {
            Expression exp = (Expression) ref;
            OJClass clazz;
            try {
                clazz = exp.getType(env);
            } catch (Exception e) {
                throw Util.newInternal(
                        e, "cannot derive type for expression '" + exp + "'");
            }
            if (clazz == null) {
                throw Util.newInternal(
                        "cannot derive type for expression '" + exp + "'");
            }
            return clazz;
        } else {
            throw Util.newInternal(
                    "cannot derive type for " + ref.getClass() + ": " + ref);
        }
    }

    public static Expression[] toArray(ExpressionList expressionList) {
        if (expressionList == null) {
            return new Expression[0];
        }
        Expression[] expressions = new Expression[expressionList.size()];
        for (int i = 0; i < expressionList.size(); i++) {
            expressions[i] = expressionList.get(i);
        }
        return expressions;
    }

    public static ExpressionList toList(Expression[] exps) {
        ExpressionList list = new ExpressionList();
        for (int i = 0; i < exps.length; i++) {
            list.add(exps[i]);
        }
        return list;
    }

    /**
     * Sets a {@link ParseTreeVisitor} going on a parse tree, and returns the
     * result.
     */
    public static ParseTree go(ParseTreeVisitor visitor, ParseTree p) {
        ObjectList holder = new ObjectList(p);
        try {
            p.accept(visitor);
        } catch (StopIterationException e) {
            // ignore the exception -- it was just a way to abort the traversal
        } catch (ParseTreeException e) {
            throw Util.newInternal(
                    e, "while visiting expression " + p);
        }
        return (ParseTree) holder.get(0);
    }

    /**
     * Sets a {@link ParseTreeVisitor} going on a given non-relational
     * expression, and returns the result.
     */
    public static Expression go(ParseTreeVisitor visitor, Expression p) {
        return (Expression) go(visitor, (ParseTree) p);
    }

    /**
     * A <code>StopIterationException</code> is a way to tell a {@link
     * openjava.ptree.util.ParseTreeVisitor} to halt traversal of the tree, but
     * is not regarded as an error.
     **/
    public static class StopIterationException extends ParseTreeException {
        public StopIterationException() {
        }
    };

    /**
     * Creates or (subsequently) retrieves a class object corresponding to the
     * declaration of an anonymous class.
     **/
    public static OJClass lookupAnonymousClass(
            ClassEnvironment env, AllocationExpression allocExp) {
        OJClass declarer = env.lookupClass(env.currentClassName());
        OJClass anonClass = (OJClass) env.mapAnonDeclToClass.get(allocExp);
        if (anonClass == null) {
            // invent a name for the anonymous class, and declare it
            String anonClassName = "_anon" + Integer.toString(
                    env.mapAnonDeclToClass.size() + 1);
            ClassDeclaration cdecl = new ClassDeclaration(
                    new ModifierList(
                            ModifierList.PRIVATE | ModifierList.FINAL),
                    anonClassName,
                    new TypeName[]{allocExp.getClassType()},
                    new TypeName[0],
                    allocExp.getClassBody());
            anonClass = new OJClass(env, declarer, cdecl);
            if (false) {
                try {
                    declarer.addClass(anonClass);
                } catch (CannotAlterException e) {
                    throw Util.newInternal(
                            e, "declarer of anonymous class must be source code");
                }
            }
            env.recordMemberClass(declarer.getName(), cdecl.getName());
            env.mapAnonDeclToClass.put(allocExp, anonClass);
        }
        return anonClass;
    }

    /**
     * Ensures that an expression is an object.  Primitive expressions are
     * wrapped in a constructor (for example, the <code>int</code> expression
     * <code>2 + 3</code> becomes <code>new Integer(2 + 3)</code>);
     * non-primitive expressions are unchanged.
     *
     * @param exp an expression
     * @param clazz <code>exp</code>'s type
     * @return a call to the constructor of a wrapper class if <code>exp</code>
     *    is primitive, <code>exp</code> otherwise
     **/
    public static Expression box(OJClass clazz, Expression exp) {
        if (clazz.isPrimitive()) {
            return new AllocationExpression(
                    clazz.primitiveWrapper(),
                    new ExpressionList(exp));
        } else {
            return exp;
        }
    }

    /**
     * Converts an expression representing a wrapped primitive into a
     * primitive.  For example, <code>new Integer(1 + 2)</code> becomes
     * <code>new Integer(1 + 2).intValue()</code>.  It is an error if the
     * expression is not a primitive type.
     *
     * @param clazz is the class of the expression. If the class is not a
     *   primitive wrapper (e.g. <code>Integer</code>), the expression is
     *   returned unchanged
     * @param exp expression to unwrap
     **/
    private static Expression unbox(OJClass clazz, Expression exp) {
        if (clazz.isPrimitiveWrapper()) {
            String s = clazz.unwrappedPrimitive().getName(); // e.g. "int"
            return new MethodCall(
                    exp,
                    s + "Value",
                    null);
        } else {
            return exp;
        }
    }


    /**
     * Converts an expression of type {@link Object} into an appropriate
     * type. If the target type is an object, generates a cast; primitive types
     * are unboxed. For example:<blockquote>
     *
     * <pre>// to convert an Object to a String, need a cast
     * while (stringIter.hasMoreElements()) {
     *   String s  = (String) stringIter.next();
     * }
     * // to convert an Object to an int, need to unbox
     * while (intIter.hasMoreElements()) {
     *   int i  = ((Integer) intIter.next()).intValue();
     * }
     * // Objects are unchanged
     * while (objectIter.hasMoreElements()) {
     *   Object i  = objectIter.next();
     * }</pre></blockquote>
     *
     * @param exp expression to unwrap
     * @param fromClazz class that expression is now
     * @param toClazz class to convert expression to
     **/
    public static Expression castObject(
            Expression exp, OJClass fromClazz, OJClass toClazz) {
        if (toClazz == fromClazz) {
            return exp;
        }
        if (toClazz == clazzObject) {
            if (!clazzObject.isAssignableFrom(fromClazz)) {
                throw Util.newInternal(
                        "cannot cast non-object " + fromClazz +
                        " to java.lang.Object");
            }
            return exp;
        } else if (toClazz.isPrimitive()) {
            // Suppose we wish to convert an Object to an int. Then we will
            // cast to an intermediate class, e.g. "java.lang.Integer", and
            // unbox. Hence we will generate
            //   ((java.lang.Integer) exp).intValue()
            //
            // todo: This may be wrong: we may already have a
            // "java.lang.Short", which has its own "intValue" method.
            OJClass intermediateClazz = toClazz.primitiveWrapper();
            return unbox(
                    intermediateClazz,
                    castObject(
                            exp,
                            fromClazz,
                            intermediateClazz));
        } else if (fromClazz.isPrimitive()) {
            return castObject(
                    box(
                            fromClazz,
                            exp),
                    fromClazz.unwrappedPrimitive(),
                    toClazz);
        } else {
            // (java.lang.String) exp
            return new CastExpression(toClazz, exp);
        }
    }

    /**
     * Converts a field access into a table.
     **/
    public static RelOptTable getTable(
        Environment env, ParseTree expr, String qualifier, String name) {
        RelOptSchema schema = getRelOptSchema(expr, env);
        if (schema == null) {
            return null;
        }
        final String[] names = qualifier == null ? new String[]{name} :
                new String[]{qualifier,name};
        return schema.getTableForMember(names);
    }

    private static RelOptSchema getRelOptSchema(ParseTree expr, Environment env) {
        if (expr instanceof Variable) {
            final Environment.VariableInfo info = env.lookupBind(expr.toString());
            RelOptSchema schema = info.getRelOptSchema();
            if (schema != null) {
                return schema;
            }
        }
        OJClass exprType = getType(env, expr);
        if (clazzConnection.isAssignableFrom(exprType)) {
            // Call the "getRelOptSchemaStatic()" method, if it exists.
            OJMethod method;
            try {
                method = exprType.getMethod("getRelOptSchemaStatic", null);
                try {
                    Object o = method.invoke(null, new Object[0]);
                    if (!(o instanceof RelOptSchema)) {
                        throw Util.newInternal(method + " must return a RelOptSchema");
                    }
                    return (RelOptSchema) o;
                } catch (IllegalAccessException e) {
                } catch (InvocationTargetException e) {
                } catch (CannotExecuteException e) {
                }
            } catch (NoSuchMemberException e) {
            }
        }
        return null;
    }

    /**
     * If the expression is an equals condition, returns the two arguments,
     * otherwise returns null.
     **/
    public static Expression[] isEquals(Expression expression) {
        if (expression instanceof BinaryExpression) {
            BinaryExpression binaryExpression = (BinaryExpression) expression;
            if (binaryExpression.getOperator() == BinaryExpression.EQUAL) {
                return new Expression[]{
                    binaryExpression.getLeft(),
                    binaryExpression.getRight()};
            }
        } else if (expression instanceof MethodCall) {
            MethodCall methodCall = (MethodCall) expression;
            if (methodCall.getName().equals("equals") &&
                    methodCall.getArguments().size() == 1) {
                return new Expression[]{
                    methodCall.getReferenceExpr(),
                    methodCall.getArguments().get(0)};
            }
        }
        return null;
    }
}

// End Toolbox.java
