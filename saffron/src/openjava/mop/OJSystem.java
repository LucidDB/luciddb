/*
 * OJSystem.java
 *
 * System environments.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1998 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.mop;


import java.util.Hashtable;
import java.util.Vector;
import java.lang.Object;
import java.io.*;
import openjava.tools.DebugOut;
import openjava.tools.parser.ParseException;
import openjava.ojc.JavaCompiler;


/**
 * <code>OJSystem</code> is the system environment: the analog of {@link
 * System} in an OpenJava system.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.System
 */
public class OJSystem
{
	/**
     * The global system environment for all public classes.
     */
    public static final GlobalEnvironment env = new GlobalEnvironment();

	/** Maps the name ({@link String}) of a class to its metaclass ({@link
	 * Class}). */
	private static Hashtable mapClassNameToMetaClass = new Hashtable();

    /** class object for primitive type boolean */
    public static OJClass BOOLEAN = OJClass.forClass(boolean.class);
    /** class object for primitive type byte */
    public static OJClass BYTE = OJClass.forClass(byte.class);
    /** class object for primitive type char */
    public static OJClass CHAR = OJClass.forClass(char.class);
    /** class object for primitive type short */
    public static OJClass SHORT = OJClass.forClass(short.class);
    /** class object for primitive type int */
    public static OJClass INT = OJClass.forClass(int.class);
    /** class object for primitive type long */
    public static OJClass LONG = OJClass.forClass(long.class);
    /** class object for primitive type float */
    public static OJClass FLOAT = OJClass.forClass(float.class);
    /** class object for primitive type double */
    public static OJClass DOUBLE = OJClass.forClass(double.class);
    /** class object for primitive type void */
    public static OJClass VOID = OJClass.forClass(void.class);

    /** class object for java.lang.String */
    public static OJClass STRING = OJClass.forClass(String.class);
    /** class object for java.lang.Object */
    public static OJClass OBJECT = OJClass.forClass(Object.class);

    /** class object for type of null literal */
    public static OJClass NULLTYPE = getNullClass();

	/**
     * The type name for null.
     */
    public static final String NULLTYPE_NAME = "<type>null";

    private static final OJClass getNullClass() {
		OJClass clazz = new OJClass();
		env.record( NULLTYPE_NAME, clazz );
		return clazz;
    }

    /**
     * The system dependent newline string.
     */
    public static final String NextLineChar = getNextLineChar();
    private static String getNextLineChar() {
        StringWriter strw = new StringWriter();
		PrintWriter pw = new PrintWriter( strw );
		pw.println();
		pw.close();
		return strw.toString();
    }

    private static final Vector additionalClasses = new Vector();

    /**
     * Adds an new public class to be generated.
     */
    public static void addNewClass( OJClass clazz ) throws MOPException {
        additionalClasses.addElement( clazz );
        OJSystem.env.record( clazz.getName(), clazz );
    }

    /* internal uses */

    private static JavaCompiler javac = null;

    /** internal use only */
    public static JavaCompiler getJavaCompiler() {
        return javac;
    }

    /** internal use only */
    public static void setJavaCompiler( JavaCompiler c ) {
        javac = c;
    }

    /** internal use only */
    public static OJClass[] addedClasses() {
        OJClass[] result = new OJClass[additionalClasses.size()];
	for (int i = 0; i < result.length; ++i) {
	    result[i] = (OJClass) additionalClasses.elementAt( i );
	}
	return result;
    }

    /** internal use only */
    public static void metabind(String clazz, String metaclazz)
	throws ParseException
    {
	/* null - default */
	if (metaclazz == null) {
	    if (mapClassNameToMetaClass.get(clazz) != null)  return;
	    metaclazz = "openjava.mop.OJClass";
	}
	Class c;
	try {
	    c = Class.forName(metaclazz);
	} catch (ClassNotFoundException e) {
	    throw new ParseException(e.toString());
	}
	DebugOut.println("class " + clazz + " : " + metaclazz);
	mapClassNameToMetaClass.put(clazz, c);
    }
    /** internal use only */
    public static Class getMetabind(String clazz) {
	Class result = searchMetaclassInTable(clazz);
	if (result == null)  result = OJClass.class;
	return result;
    }

    private static Class searchMetaclassInTable(String clazz) {
	/* exactly specified name */
	{
	    Class result = (Class) mapClassNameToMetaClass.get(clazz);
	    if (result != null)  return result;
	}
	/* a name specified using "*" (all classes in a package)
	 * More specific indication has more priority.
	 * e.g. "java.lang.*" is stronger than "java.*"
	 */
	{
	    Class result = (Class) mapClassNameToMetaClass.get(toPackageSuffix(clazz) + "*");
	    if (result != null)  return result;
	}
	/* a name specified using "-" (all subpackages)
	 * More specific indication has more priority.
	 * e.g. "java.lang.-" is stronger than "java.-"
	 */
	{
	    Class result = (Class) mapClassNameToMetaClass.get(toPackageSuffix(clazz) + "-");
	    if (result != null)  return result;
	}
	for (String pack = toPackage(clazz); pack != null;
	     pack = toPackage(pack))
	{
	    Class result = (Class) mapClassNameToMetaClass.get(toPackageSuffix(pack) + "-");
	    if (result != null)  return result;
	}
	/* nothing found */
	return null;
    }
    /* returns "java.lang." for "java.lang.String" and "" for "String" */
    private static String toPackageSuffix(String classname) {
	int pack = classname.lastIndexOf(".") + 1;
	return classname.substring(0, pack);
    }
    /* returns "java.lang" for "java.lang.String" and null for "String" */
    private static String toPackage(String classname) {
	int pack = classname.lastIndexOf(".");
	if (pack == -1)  return null;
	return classname.substring(0, pack);
    }

    /** internal use only */
    public static Object orderingLock;
    /** internal use only */
    public static OJClass waited = null;
    /** internal use only */
    public static final Hashtable underConstruction = new Hashtable();
    /** internal use only */
    public static final Vector waitingPool = new Vector();
}
