/*
 * OJClass.java
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


import java.io.InputStream;
import java.io.Writer;
import java.io.IOException;
import java.util.*;
import java.lang.reflect.*;
import openjava.ptree.*;
import openjava.ptree.util.PartialParser;
import openjava.ptree.util.TypeNameQualifier;
import openjava.tools.DebugOut;
import openjava.syntax.SyntaxRule;


/**
 * The <code>OJClass</code> class represents a class metaobject.  If
 * the class has its valid .class file in CLASSPATH, the metaobject can
 * behave like <code>java.lang.Class</code>.  If the class has its
 * valid .oj file in the packages where some classes are being compiled
 * by OpenJava system, or in the packages explicitly specified by
 * something like OpenJava compiler option or environment variable, the
 * metaobject can have information about source code just as
 * stamements, expressions, and etc.
 * <p>
 * Additionaly, you can overrides the methods for introspection.
 * <pre>
 *     OJClass[] getDeclaredClasses()
 *     OJMethod[] getDeclaredMethods()
 *     OJField[] getDeclaredFields()
 *     OJConstructor[] getDeclaredConstructors()
 *     OJMethod getAcceptableMethod(OJClass,String,OJClass[])
 *     OJMethod getAcceptableConstructor(OJClass,String,OJClass[])
 * </pre>
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Class
 * @see openjava.mop.OJMethod
 * @see openjava.mop.OJField
 * @see openjava.mop.OJConstructor
 *
 **/
public class OJClass implements OJMember
{

    private OJClassImp substance;

    /* -- constructors -- */

    /**
     * Generates a metaobject from source code.
     * <p>
     * This constructor will be invoked by the OpenJava system.
     * In inheriting this class, you must call this consturctor
     * from a constructor with the same signature in your class.
     * <p>
     * For example, in defining a subclass <code>YourClass</code>
     * derived from this class, at least you have to write as follows:
     * <pre>
     * public YourClass( Environment outer_env, OJClass declarer,
     *                   ClassDeclaration ptree ) {
     *     super( outer_env, declarer, ptree );
     * }
     * </pre>
     *
     * @param  outer_env  environment of this class metaobject
     * @param  declarer  the class metaobject declaring this
     *	       class metaobject.
     * @param  ptree  the parse tree representing this class metaobject.
     **/
    public OJClass( Environment outer_env, OJClass declarer,
		    ClassDeclaration ptree )
    {
	substance = new OJClassSourceCode( this, outer_env, declarer, ptree );
    }

    /**
     * Generates a metaobject from byte code.
     * <p>
     * This constructor will be invoked only by the OpenJava system.
     * In inheriting this class, you must call this consturctor
     * from a constructor with the same signature in your class.
     * <p>
     * For example, in defining a subclass <code>YourClass</code>
     * derived from this class, at least you have to write as follows:
     * <pre>
     * public YourClass( Class java_class, MetaInfo metainfo ) {
     *     super( java_class, metainfo );
     * }
     * </pre>
     *
     * @param  outer_env  environment of this class metaobject
     * @param  declarer  the class metaobject declaring this
     *	       class metaobject.
     * @param  ptree  the parse tree representing this class metaobject.
     */
    public OJClass( Class java_class, MetaInfo metainfo ) {
	substance = new OJClassByteCode( java_class, metainfo );
    }

    /**
     * For arrays
     */
    private OJClass( OJClass componentType ) {
	substance = new OJClassArray( componentType );
    }

    public static OJClass arrayOf( OJClass componentType ) {
	return new OJClass( componentType );
    }

    /**
     * For dummy type of null object.
     * This should be called only once by OJSystem.initConstants().
     */
    OJClass() {
	substance = new OJClassNull();
    }

    /* -- Introspection Part -- */

    /**
     * Returns the <code>OJClass</code> object associated with the class
     * with the given string name.
     * Given the fully-qualified name for a class or interface, this
     * method attempts to locate, load and link the class.  If it
     * succeeds, returns the Class object representing the class.  If
     * it fails, the method throws a OJClassNotFoundException.
     * <p>
     * For example, the following code fragment returns the runtime
     * <code>OJClass</code> descriptor for the class named
     * <code>java.lang.Thread</code>:
     * <ul><code>
     *     OJClass t = OJClass.forName( "java.lang.Thread" );
     * </code></ul>
     *
     * @param  class_name  the fully qualified name of the desired class.
     * @return  the <code>OJClass</code> descriptor for the class with
     *		the specified name.
     * @exception  OJClassNotFoundException  if the class could not be found.
     * @since %SOFTWARE% 1.0
     **/
    public static OJClass forName( String name )
        throws OJClassNotFoundException
    {
	DebugOut.println( "OJClass.forName(\"" + name.toString() + "\")" );

	name = nameForJavaClassName( name );

        OJClass result;

	result = OJSystem.env.lookupClass( name );
        if (result != null)  return result;

	if (isArrayName( name )) {
	    OJClass component = forName( stripBrackets( name ) );
	    result = new OJClass( component );
	    OJSystem.env.record( name, result );
	    return result;
	}

	result = lookupFromByteCode( nameToJavaClassName( name ) );
	if (result != null) {
	    OJSystem.env.record( name, result );
	    return result;
	}

	throw new OJClassNotFoundException( name );
    }

    private static final OJClass lookupFromByteCode( String name ) {
	OJClass result = null;
	try {
	    Class javaClass = Class.forName( name );

	    result = forClass( javaClass );
	    if (result != null) {
	        OJSystem.env.record( name, result );
	        return result;
	    }
	} catch ( ClassNotFoundException e ) {
	    int dot = name.lastIndexOf( '.' );
	    if (dot == -1)  return null;
	    String innername = replaceDotWithDollar( name, dot );
	    result = lookupFromByteCode( innername );
	} catch ( NoClassDefFoundError e ) {
	    int dot = name.lastIndexOf( '.' );
	    if (dot == -1)  return null;
	    String innername = replaceDotWithDollar( name, dot );
	    result = lookupFromByteCode( innername );
	}
	return result;
    }

    public static final String replaceDotWithDollar( String base, int i ) {
	return base.substring( 0, i ) + '$' + base.substring( i + 1 );
    }

    private static final boolean isArrayName( String name ) {
        return (name.startsWith( "[" ) || name.endsWith( "[]" ));
    }

    private static final String stripBrackets( String ojcname ) {
        return ojcname.substring( 0, ojcname.length() - 2 );
    }

    private static final String nameForJavaClassName( String jcname ) {
	return Toolbox.nameForJavaClassName( jcname );
    }

    private static final String nameToJavaClassName( String ojcname ) {
	return Toolbox.nameToJavaClassName( ojcname );
    }

    static OJClass[] arrayForClasses( Class[] jclasses ) {
        OJClass[] result = new OJClass[jclasses.length];
        for (int i = 0; i < result.length; ++i) {
	    result[i] = forClass( jclasses[i] );
	}
        return result;
    }

    static Class[] toClasses( OJClass[] classes )
    {
        Class[] result = new Class[classes.length];
        for (int i = 0; i < result.length; ++i) {
	    result[i] = classes[i].getCompatibleJavaClass();
	}
        return result;
    }

    /**
     * Converts a <code>OJClass</code> object to an <code>OJClass</code>
     * object.
     * <p>
     * This method returns the same <code>OJClass</code> object
     * whenever it is invoked for the same <code>Class</code>object.
     * It gurantees one-to-one correspondence between <code>Class</code>
     * and <code>OJClass</code>.
     */
    public static OJClass forClass( Class javaClass ) {
        if (javaClass == null)  return null;

	String name = nameForJavaClassName( javaClass.getName() );

	OJClass result = OJSystem.env.lookupClass( name );
        if (result != null)  return result;

	if (isArrayName( name )) {
	    try {
	        OJClass component = forName( stripBrackets( name ) );
		result = new OJClass( component );
		OJSystem.env.record( name, result );
		return result;
	    } catch ( Exception e ) {
		System.err.println( "OJClass.forClass(" + name + ") : " + e );
		/* continue as a non array */
	    }
	}

	result = lookupFromMetaInfo( javaClass );
	OJSystem.env.record( name, result );

        return result;
    }

    private static final OJClass lookupFromMetaInfo( Class javaClass ) {
	MetaInfo metainfo = new MetaInfo( javaClass );
	String mcname = metainfo.get( "instantiates" );
	DebugOut.println( javaClass + " is an instance of " + mcname );
	try {
	    Class metaclazz = Class.forName( mcname );
	    Class[] paramtypes = new Class[] { Class . class,
					       MetaInfo . class };
	    Constructor constr = metaclazz.getConstructor( paramtypes );
	    Object[] args = new Object[]{ javaClass, metainfo };
	    return (OJClass) constr.newInstance( args );
	} catch ( ClassNotFoundException e ) {
	    System.err.println( "metaclass " + mcname +
			       " for " + javaClass + " not found." +
			       " substituted by default metaclass." );
	    metainfo = new MetaInfo( javaClass.getName() );
	    return new OJClass( javaClass, metainfo );
	} catch ( Exception e ) {
	    System.err.println( "metaclass " + mcname + " doesn't provide" +
			       " proper constructor for bytecode." +
			       " substituted by default metaclass. : " + e );
	    metainfo = new MetaInfo( javaClass.getName() );
	    return new OJClass( javaClass, metainfo );
	}
    }

    /**
     * Converts <code>ParseTree</code> objects to an <code>OJClass</code>
     * object.  The generated <code>OJClass</code> object is to be
     * registered as globally asscessible class but not to appear as
     * generated source code.
     * <p>
     */
    public static OJClass forParseTree(Environment env, OJClass declaringClass,
				        ClassDeclaration ptree)
	throws AmbiguousClassesException, ClassNotFoundException
    {
        String qname;
        if (declaringClass == null) {
            qname = env.toQualifiedName(ptree.getName());
        } else {
            qname = env.currentClassName() +
      	        "." + ptree.getName();
        }

        Class metaclazz = null;
        if (ptree.getMetaclass() != null) {
            String mcname = env.toQualifiedName(ptree.getMetaclass());
            metaclazz = Class.forName(mcname);
        } else {
            metaclazz = OJSystem.getMetabind(qname);
        }

        OJClass result;
	try {
	    Constructor constr
		= metaclazz.getConstructor( new Class[]{
		    Environment . class,
		    OJClass . class,
		    ClassDeclaration . class } );
	    Object[] args = new Object[]{env, declaringClass, ptree};
	    result = (OJClass) constr.newInstance(args);
	} catch (NoSuchMethodException ex) {
	    System.err.println("errors during gererating a metaobject for " +
			       ptree.getName() + " : " + ex);
	    result = new OJClass(env, declaringClass, ptree);
	} catch (InvocationTargetException ex) {
	    System.err.println("errors during gererating a metaobject for " +
			       ptree.getName() + " : " + ex.getTargetException());
            ex.printStackTrace();
	    result = new OJClass(env, declaringClass, ptree);
	} catch (Exception ex) {
	    System.err.println("errors during gererating a metaobject for " +
			       ptree.getName() + " : " + ex);
	    result = new OJClass(env, declaringClass, ptree);
	}

	OJClass existing = OJSystem.env.lookupClass(qname);

	if (existing != null) {
	    throw new AmbiguousClassesException(qname);
	}

        OJSystem.env.record(qname, result);
        return result;
    }

    public static OJClass forObject( Object obj ) {
        return forClass( obj.getClass() );
    }

    /**
     * Generates a expression parse tree from a given
     * <code>String</code> object under the given environment.
     *
     * @param  env  an environment for the source code.
     * @param  str  a fragment of source code representing an expression.
     * @return an expression parse tree
     */
    protected static final Expression
    makeExpression( Environment env, String str )
	throws MOPException
    {
        return PartialParser.makeExpression( env, str );
    }

    /**
     * Generates an expression parse tree from a given
     * <code>String</code> object under the environment of
     * this class object.
     *
     * @param  str  a fragment of source code representing an expression.
     * @return an expression parse tree
     */
    protected final Expression makeExpression( String str )
	throws MOPException
    {
        return makeExpression( getEnvironment(), str );
    }

    /**
     * Generates a statement parse tree from a given
     * <code>String</code> object under the given environment.
     *
     * @param  env  an environment for the source code.
     * @param  str  a fragment of source code representing a statement.
     * @return a statement parse tree
     */
    protected static final Statement
    makeStatement( Environment env, String str )
	throws MOPException
    {
        return PartialParser.makeStatement( env, str );
    }

    /**
     * Generates a statement parse tree from a given
     * <code>String</code> object under the environment of
     * this class object.
     *
     * @param  str  a fragment of source code representing a statement.
     * @return a statement parse tree
     */
    protected final Statement makeStatement( String str )
	throws MOPException
    {
        return makeStatement( getEnvironment(), str );
    }

    /**
     * Generates a statement list parse tree from a given
     * <code>String</code> object under the given environment.
     *
     * @param  env  an environment for the source code.
     * @param  str  a fragment of source code representing a statement list.
     * @return a statement list parse tree
     */
    protected static final StatementList
    makeStatementList( Environment env, String str )
	throws MOPException
    {
        return PartialParser.makeStatementList( env, str );
    }

    /**
     * Generates a statement list parse tree from a given
     * <code>String</code> object under the environment of
     * this class object.
     *
     * @param  str  a fragment of source code representing a statement list.
     * @return a statement list parse tree
     */
    protected final StatementList makeStatementList( String str )
	throws MOPException
    {
        return makeStatementList( getEnvironment(), str );
    }

    /**
     * Converts the object to a string. The string representation is
     * the string "class" or "interface", followed by a space, and then
     * by the fully qualified name of the class in the format returned
     * by <code>getName</code>.  If this <code>OJClass</code> object
     * represents a primitive type, this method returns the name of the
     * primitive type.  If this <code>OJClass</code> object represents
     * void this method returns "void".
     *
     * @return a string representation of this class object.
     */
    public String toString() {
	return substance.toString();
    }

    /**
     * Obtains an environment of this class object.
     * This environment has information about members and outr
     * environments such as packages but not local information like
     * local variable in methods of this class.
     *
     * @return  an environment of this class object.
     */
    public Environment getEnvironment() {
        return substance.getEnvironment();
    }

    public Object newInstance()
  	throws InstantiationException, IllegalAccessException,
	       CannotExecuteException
    {
	return substance.newInstance();
    }

    public boolean isInstance( Object obj ) {
	if (obj == null)  return false;
	return this.isAssignableFrom( OJClass.forObject( obj ) );
    }

    /**
     * Determines if the class or interface represented by this
     * <code>OJClass</code> object is either the same as, or is a
     * superclass or superinterface of, the class or interface
     * represented by the specified <code>OJClass</code> parameter. It
     * returns <code>true</code> if so; otherwise it returns
     * <code>false</code>. If this <code>OJClass</code> object
     * represents a primitive type, this method returns
     * <code>true</code> if the type represented by this
     * <code>OJClass</code> object can accept the type represented by
     * the specified <code>OJClass</code> parameter; otherwise it
     * returns <code>false</code>.
     *
     * <p> Specifically, this method tests whether the type
     * represented by the specified <code>OJClass</code> parameter can
     * be converted to the type represented by this
     * <code>OJClass</code> object via an identity conversion or via a
     * widening reference/primitive conversion.  <p> The behavior
     * about primitive types is different from the method of the same
     * name of <code>java.lang.Class</code>.
     *
     * @exception NullPointerException if the specified class parameter is
     *            null.
     */
    public boolean isAssignableFrom(OJClass clazz) {
	if (clazz.toString() == OJSystem.NULLTYPE_NAME)  return true;
	if (clazz == this) {
	     /* if it is exactly the same type */
	    return true;
	}
	if (this.isPrimitive()) {
	    /* the java.lang.Class's returns always false */
	    if (! clazz.isPrimitive())  return false;
	    if (clazz == OJSystem.CHAR) {
		return (primitiveTypeWidth(this)
		    > primitiveTypeWidth(OJSystem.SHORT));
	    }
	    if (primitiveTypeWidth(this) > primitiveTypeWidth(OJSystem.VOID)) {
		return (primitiveTypeWidth(this) > primitiveTypeWidth(clazz));
	    }
	    return false;
	} else {
	    if (clazz.isPrimitive())  return false;
	}
	/* now class is a reference type */
	if (this == OJSystem.OBJECT)  return true;
	if (this.isArray()) {
	    if (! clazz.isArray())  return false;
	    OJClass comp = this.getComponentType();
	    return comp.isAssignableFrom(clazz.getComponentType());
	} else {
	    if (clazz.isArray())  return false;
	}
	/* getInterfaces() returns only the declared intefaces
	 * So the interfaces of the superclasses should be checked.
	 */
	if (this.isInterface()) {
	    /* for an assigning class which is either interface or class */
	    OJClass[] faces = clazz.getInterfaces();
	    for (int i = 0; i < faces.length; ++i) {
		if (isAssignableFrom(faces[i]))  return true;
	    }
	}
	/* now this is a class */
	if (clazz.isInterface())  return false;
	OJClass base = clazz.getSuperclass();
	return (base == null) ? false : isAssignableFrom(base);
    }

    private static int primitiveTypeWidth(OJClass ptype) {
	if (ptype == OJSystem.BYTE)  return 1;
	if (ptype == OJSystem.SHORT) return 2;
	if (ptype == OJSystem.INT)  return 3;
	if (ptype == OJSystem.LONG)  return 4;
	if (ptype == OJSystem.FLOAT)  return 5;
	if (ptype == OJSystem.DOUBLE)  return 5;
	return -1;
    }

    /**
     * Determines if the specified <code>OJClass</code> object represents
     * an interface type.
     *
     * @return  <code>true</code> if this object represents an interface;
     *          <code>false</code> otherwise.
     */
    public boolean isInterface() {
	return substance.isInterface();
    }

    /**
     * Determines if this <code>OJClass</code> object represents an
     * array class.
     *
     * @return  <code>true</code> if this object represents an array class;
     *          <code>false</code> otherwise.
     */
    public boolean isArray() {
	return substance.isArray();
    }

    /**
     * Determines if the specified <code>OJClass</code> object represents a
     * primitive type.
     *
     * <p> There are nine predefined <code>OJClass</code> objects to represent
     * the eight primitive types and void.  These are created by the Java
     * Virtual Machine, and have the same names as the primitive types that
     * they represent, namely <code>boolean</code>, <code>byte</code>,
     * <code>char</code>, <code>short</code>, <code>int</code>,
     * <code>long</code>, <code>float</code>, and <code>double</code>.
     *
     * <p> These objects may be accessed via the following public static
     * final variables, and are the only <code>OJClass</code> objects for
     * which this method returns <code>true</code>.
     *
     * @see     openjava.mop.OJSystem#BOOLEAN
     * @see     openjava.mop.OJSystem#CHAR
     * @see     openjava.mop.OJSystem#BYTE
     * @see     openjava.mop.OJSystem#SHORT
     * @see     openjava.mop.OJSystem#INT
     * @see     openjava.mop.OJSystem#LONG
     * @see     openjava.mop.OJSystem#FLOAT
     * @see     openjava.mop.OJSystem#DOUBLE
     * @see     openjava.mop.OJSystem#VOID
     *
     * @return  <code>true</code> if this object represents a primitive type;
     *          <code>false</code> otherwise.
     */
    public boolean isPrimitive() {
	return substance.isPrimitive();
    }

    /**
     * Determines if the specified <code>OJClass</code> object represents a
     * wrapper class for a primitive type.
     *
     * @return  <code>true</code> if this object represents a wrapper class
     *          for a primitive type; <code>false</code> otherwise.
     */
    public boolean isPrimitiveWrapper() {
	return (this != unwrappedPrimitive());
    }

    /**
     * Obtains the wrapper class if this class represents a primitive
     * type.
     * <p>
     * For example this method returns java.lang.Integer for int.
     *
     * @return The wrapper class for this primitive type.
     *         <code>null</code> for void.
     */
    public OJClass primitiveWrapper() {
	if (this == OJSystem.VOID)  return null;
	if (this == OJSystem.BOOLEAN)
	    return OJClass.forClass( java.lang.Boolean.class );
	if (this == OJSystem.BYTE)
	    return OJClass.forClass( java.lang.Byte.class );
	if (this == OJSystem.CHAR)
	    return OJClass.forClass( java.lang.Character.class );
	if (this == OJSystem.SHORT)
	    return OJClass.forClass( java.lang.Short.class );
	if (this == OJSystem.INT)
	    return OJClass.forClass( java.lang.Integer.class );
	if (this == OJSystem.LONG)
	    return OJClass.forClass( java.lang.Long.class );
	if (this == OJSystem.FLOAT)
	    return OJClass.forClass( java.lang.Float.class );
	if (this == OJSystem.DOUBLE)
	    return OJClass.forClass( java.lang.Double.class );
	//otherwise returns as is
	return this;
    }

    /**
     * Obtains the real type class if this class represents a primitive
     * wrapper type.
     * <p>
     * For example this method returns int for java.lang.Integer.
     *
     * @return The real primitive type for this primitive wrapper class.
     */
    public OJClass unwrappedPrimitive() {
	if (this == OJClass.forClass( java.lang.Boolean.class ))
	    return OJSystem.BOOLEAN;
	if (this == OJClass.forClass( java.lang.Byte.class ))
	    return OJSystem.BYTE;
	if (this == OJClass.forClass( java.lang.Character.class ))
	    return OJSystem.CHAR;
	if (this == OJClass.forClass( java.lang.Short.class ))
	    return OJSystem.SHORT;
	if (this == OJClass.forClass( java.lang.Integer.class ))
	    return OJSystem.INT;
	if (this == OJClass.forClass( java.lang.Long.class ))
	    return OJSystem.LONG;
	if (this == OJClass.forClass( java.lang.Float.class ))
	    return OJSystem.FLOAT;
	if (this == OJClass.forClass( java.lang.Double.class ))
	    return OJSystem.DOUBLE;
	//otherwise returns as is
	return this;
    }

    /**
     * Returns the fully-qualified name of the entity (class,
     * interface, array class, primitive type, or void) represented by
     * this <code>OJClass</code> object, as a <code>String</code>.
     *
     * <p> If this <code>OJClass</code> object represents a class of
     * arrays, then the internal form of the name consists of the name
     * of the element type in Java signature format, followed by one
     * or more "<tt>[]</tt>" characters representing the depth of array
     * nesting. This representation differs from that of
     * <code>java.lang.Class.forName()</code>. Thus:
     *
     * <blockquote><pre>
     * OJClass.forClass( (new int[3][4]).getClass() ).getName()
     * </pre></blockquote>
     *
     * returns "<code>java.lang.Object[]</code>" and:
     *
     * <blockquote><pre>
     * OJClass.forClass( (new int[3][4]).getClass() ).getName()
     * </pre></blockquote>
     *
     * returns "<code>int[][]</code>".
     *
     * <p> The class or interface name <tt><i>classname</i></tt> is
     * given in fully qualified form as shown in the example above.
     *
     * @return  the fully qualified name of the class or interface
     *          represented by this object.
     */
    public String getName() {
	return substance.getName();
    }

    /**
     * Returns the simple name of the class, interface or array class
     * represented by this <code>OJClass</code> object, as a
     * <code>String</code>.  Thus:
     *
     * <blockquote><pre>
     * OJClass.forClass( (new Object[3]).getClass() ).getName()
     * </pre></blockquote>
     *
     * returns "<code>Object</code>".
     *
     * @return  the simple name of the class or interface
     *          represented by this object.
     */
    public String getSimpleName() {
        return Environment.toSimpleName( getName() );
    }

    /**
     * Gets the package name for this class as a <code>String</code>.
     * Null is returned if its package was not specified in source code of
     * this class.
     *
     * @return the package name of the class, or null if its package
     *         was not specified in source code.
     */
    public String getPackage() {
	int last = getName().lastIndexOf( '.' );
	if (last == -1)  return null;
	return getName().substring( 0, last );
    }

    /**
     * Determines if the specified class object is in the same package
     * as this class object.
     *
     * <p>If null is given, this method returns alway false.
     *
     * @param c the class object to test against this class object.
     * @return true in case that both class is is the sample package.
     */
    public boolean isInSamePackage( OJClass c ) {
	if (c == null)  return false;
	String pack = c.getPackage();
	if (pack == null)  return (getPackage() == null);
	return pack.equals( getPackage() );
    }

    public ClassLoader getClassLoader() throws CannotInspectException {
	return substance.getClassLoader();
    }

    /**
     * Returns the <code>OJClass</code> representing the superclass of
     * the entity (class, interface, primitive type or void)
     * represented by this <code>OJClass</code>.  If this
     * <code>OJClass</code> represents either the
     * <code>java.lang.Object</code> class, an interface, a primitive
     * type, or void, then null is returned.  If this object
     * represents an array class then the <code>OJClass</code> object
     * representing the <code>java.lang.Object</code> class is
     * returned.
     *
     * @return  the superclass of the class represented by this object.
     */
    public OJClass getSuperclass() {
	return substance.getSuperclass();
    }

    /**
     * Determines the interfaces implemented by the class or interface
     * represented by this object.
     *
     * <p> If this object represents a class, the return value is an array
     * containing objects representing all interfaces implemented by the
     * class. The order of the interface objects in the array corresponds to
     * the order of the interface names in the <code>implements</code> clause
     * of the declaration of the class represented by this object. For
     * example, given the declaration:
     * <blockquote><pre>
     * class Shimmer implements FloorWax, DessertTopping { ... }
     * </pre></blockquote>
     * suppose the value of <code>clazz</code> is an class object for
     * the class <code>Shimmer</code>; the value of the expression:
     * <blockquote><pre>
     * clazz.getInterfaces()[0]
     * </pre></blockquote>
     * is the <code>OJClass</code> object that represents interface
     * <code>FloorWax</code>; and the value of:
     * <blockquote><pre>
     * clazz.getInterfaces()[1]
     * </pre></blockquote>
     * is the <code>OJClass</code> object that represents interface
     * <code>DessertTopping</code>.
     *
     * <p> If this object represents an interface, the array contains
     * objects representing all interfaces extended by the
     * interface. The order of the interface objects in the array
     * corresponds to the order of the interface names in the
     * <code>extends</code> clause of the declaration of the interface
     * represented by this object.
     *
     * <p> If this object represents a class or interface that
     * implements no interfaces, the method returns an array of length
     * 0.
     *
     * <p> If this object represents a primitive type or void, the
     * method returns an array of length 0.
     *
     * <p> To be <code>getDeclaredInterfaces()<code>.
     *
     * @return an array of interfaces implemented by this class object.
     */
    public OJClass[] getInterfaces() {
	return substance.getInterfaces();
    }

    /**
     * Returns the <code>OJClass</code> representing the component type of an
     * array.  If this class does not represent an array class this method
     * returns null.
     *
     * @return the class object representing the type of component of this
     *         array.
     */
    public OJClass getComponentType() {
	return substance.getComponentType();
    }

    /**
     * Returns the Java language modifiers and the user defined modifiers
     * for this class or interface, as a <code>OJModifier</code> object.
     *
     * <p> If the underlying class is an array class, then its
     * <code>public</code>, <code>private</code> and <code>protected</code>
     * modifiers are the same as those of its component type.  If this
     * <code>OJClass</code> represents a primitive type or void, its
     * <code>public</code> modifier is always <code>true</code>, and its
     * <code>protected</code> and <code>private</code> modifers are always
     * <code>false</code>. If this object represents an array class, a
     * primitive type or void, then its <code>final</code> modifier is always
     * <code>true</code> and its interface modifer is always
     * <code>false</code>. The values of its other modifiers are not determined
     * by this specification.
     *
     * @see     openjava.mop.OJModifier
     *
     * @return the OJModifier object representing the modifiers of this class
     *         object
     */
    public OJModifier getModifiers() {
	return substance.getModifiers();
    }

    /**
     * Obtains an parse tree of suffix in extended syntax starting
     * with the specified keyword.  Returned
     * <code>openjava.ptree.ParseTree</code> object has a structure
     * built by an <code>openjava.syntax.SyntaxRule</code> object
     * returned via the method <code>getDeclSuffixRule(String)</code>.
     *
     * @see openjava.mop.OJClass#getDeclSuffixRule(String)
     * @see openjava.syntax.SyntaxRule
     *
     * @return the parse tree
     */
    public ParseTree getSuffix( String keyword ) {
	return substance.getSuffix( keyword );
    }

    public Object[] getSigners() throws CannotExecuteException {
	return substance.getSigners();
    }

    /**
     * If the class or interface represented by this
     * <code>OJClass</code> object is a member of another class,
     * returns the <code>OJClass</code> object representing the class
     * in which it was declared.  This method returns null if this
     * class or interface is not a member of any other class.  If this
     * <code>OJClass</code> object represents an array class, a
     * primitive type, or void, then this method returns null.
     *
     * @return  the class object declaring this class object.
     */
    public OJClass getDeclaringClass() {
	return substance.getDeclaringClass();
    }

    public final OJClass[] getAllClasses() {
	return overridesOn( getDeclaredClasses(), getInheritedClasses() );
    }

    public final OJField[] getAllFields() {
	return overridesOn( getDeclaredFields(), getInheritedFields() );
    }

    public final OJMethod[] getAllMethods() {
	return overridesOn( getDeclaredMethods(), getInheritedMethods() );
    }

    public OJClass[] getInheritedClasses() {
	OJClass base = getSuperclass();
	if (base == null) {
	    return new OJClass[0];
	} else {
	    return base.getInheritableClasses( this );
	}
    }

    public OJField[] getInheritedFields() {
	OJClass base = getSuperclass();
	OJField[] base_f;
	if (base == null) {
	    base_f = new OJField[0];
	} else {
	    base_f = base.getInheritableFields( this );
	}
	int len = base_f.length;
	OJClass[] faces = getInterfaces();
	OJField[][] face_fs = new OJField[faces.length][];
	for (int i = 0; i < faces.length; ++i) {
	    face_fs[i] = faces[i].getInheritableFields( this );
	    len += face_fs[i].length;
	}
	OJField[] result = new OJField[len];
	int count = 0;
	for (int i = 0; i < faces.length; ++i) {
	    System.arraycopy( face_fs[i], 0, result, count,
			      face_fs[i].length );
	    count += face_fs[i].length;
	}
	System.arraycopy( base_f, 0, result, count, base_f.length );
	return result;
    }

    public final OJMethod[] getInheritedMethods() {
	OJClass base = getSuperclass();
	OJMethod[] base_m;
	if (base == null) {
	    base_m = new OJMethod[0];
	} else {
	    base_m = base.getInheritableMethods( this );
	}
	int len = base_m.length;
	OJClass[] faces = getInterfaces();
	OJMethod[][] face_ms = new OJMethod[faces.length][];
	for (int i = 0; i < faces.length; ++i) {
	    face_ms[i] = faces[i].getInheritableMethods( this );
	    len += face_ms[i].length;
	}
	OJMethod[] result = new OJMethod[len];
	int count = 0;
	for (int i = 0; i < faces.length; ++i) {
	    System.arraycopy( face_ms[i], 0, result, count,
			      face_ms[i].length );
	    count += face_ms[i].length;
	}
	System.arraycopy( base_m, 0, result, count, base_m.length );
	return result;
    }

    /**
     * Use <code>getInheritableClasses(OJClass)</code>
     * @deprecated
     * @see #getInheritableClasses(OJClass)
     */
    public final OJClass[] getInheritableClasses() {
	OJClass[] nonprivates = removeThePrivates( getAllClasses() );
	return removeTheDefaults( nonprivates );
    }

    /**
     * Use <code>getInheritableFields(OJClass)</code>
     * @deprecated
     * @see #getInheritableFields(OJClass)
     */
    public final OJField[] getInheritableFields() {
	OJField[] nonprivates = removeThePrivates( getAllFields() );
	return removeTheDefaults( nonprivates );
    }

    /**
     * Use <code>getInheritableMethods(OJClass)</code>
     * @deprecated
     * @see #getInheritableMethods(OJClass)
     */
    public final OJMethod[] getInheritableMethods() {
	OJMethod[] nonprivates = removeThePrivates( getAllMethods() );
	return removeTheDefaults( nonprivates );
    }

    public OJClass[] getInheritableClasses( OJClass situation ) {
        OJClass[] result = removeThePrivates( getAllClasses() );
	if (isInSamePackage( situation ))  return result;
	return removeTheDefaults( result );
    }

    public OJField[] getInheritableFields( OJClass situation ) {
        OJField[] result = removeThePrivates( getAllFields() );
	if (isInSamePackage( situation ))  return result;
	return removeTheDefaults( result );
    }

    public OJMethod[] getInheritableMethods( OJClass situation ) {
        OJMethod[] result = removeThePrivates( getAllMethods() );
	if (isInSamePackage( situation ))  return result;
	return removeTheDefaults( result );
    }

    public OJConstructor[] getInheritableConstructors( OJClass situation ) {
        OJConstructor[] result
	    = removeThePrivates( getDeclaredConstructors() );
	if (isInSamePackage( situation ))  return result;
	return removeTheDefaults( result );
    }

    private static final OJClass[]
    overridesOn( OJClass[] declareds, OJClass[] bases ) {
	return Toolbox.overridesOn( declareds, bases );
    }

    private static final OJField[]
    overridesOn( OJField[] declareds, OJField[] bases ) {
	return Toolbox.overridesOn( declareds, bases );
    }

    private static final OJMethod[]
    overridesOn( OJMethod[] declareds, OJMethod[] bases ) {
	return Toolbox.overridesOn( declareds, bases );
    }

    private static final OJClass[]
    removeThePrivates( OJClass[] src_classes ) {
	return Toolbox.removeThePrivates( src_classes );
    }

    private static final OJField[]
    removeThePrivates( OJField[] src_fields ) {
	return Toolbox.removeThePrivates( src_fields );
    }

    private static final OJMethod[]
    removeThePrivates( OJMethod[] src_methods ) {
	return Toolbox.removeThePrivates( src_methods );
    }

    private static final OJConstructor[]
    removeThePrivates( OJConstructor[] src_constrs ) {
	return Toolbox.removeThePrivates( src_constrs );
    }

    private static final OJClass[]
    removeTheDefaults( OJClass[] src_classes ) {
	return Toolbox.removeTheDefaults( src_classes );
    }

    private static final OJField[]
    removeTheDefaults( OJField[] src_fields ) {
	return Toolbox.removeTheDefaults( src_fields );
    }

    private static final OJMethod[]
    removeTheDefaults( OJMethod[] src_methods ) {
	return Toolbox.removeTheDefaults( src_methods );
    }

    private static final OJConstructor[]
    removeTheDefaults( OJConstructor[] src_constrs ) {
	return Toolbox.removeTheDefaults( src_constrs );
    }

    private static final OJClass[]
    removeTheNonPublics( OJClass[] src_classes ) {
	return Toolbox.removeTheNonPublics( src_classes );
    }

    private static final OJField[]
    removeTheNonPublics( OJField[] src_fields ) {
	return Toolbox.removeTheNonPublics( src_fields );
    }

    private static final OJMethod[]
    removeTheNonPublics( OJMethod[] src_methods ) {
	return Toolbox.removeTheNonPublics( src_methods );
    }

    private static final OJConstructor[]
    removeTheNonPublics( OJConstructor[] src_constrs ) {
	return Toolbox.removeTheNonPublics( src_constrs );
    }

    private static final OJMethod[]
    pickupMethodsByName( OJMethod[] src_methods, String name ) {
	return Toolbox.pickupMethodsByName( src_methods, name );
    }

    private static final OJField
    pickupField( OJField[] src_fields, String name ) {
	return Toolbox.pickupField( src_fields, name );
    }

    private static final OJMethod
    pickupMethod( OJMethod[] src_methods, String name,
		  OJClass[] parameterTypes )
    {
        return Toolbox.pickupMethod( src_methods, name, parameterTypes );
    }

    private static final OJConstructor
    pickupConstructor( OJConstructor[] src_constrs,
		       OJClass[] parameterTypes )
    {
        return Toolbox.pickupConstructor( src_constrs, parameterTypes );
    }

    private static final OJMethod
    pickupAcceptableMethod( OJMethod[] src_methods, String name,
			    OJClass[] parameterTypes )
    {
        return Toolbox.pickupAcceptableMethod( src_methods,
					       name, parameterTypes );
    }

    private static final OJConstructor
    pickupAcceptableConstructor( OJConstructor[] src_constrs,
                                 OJClass[] parameterTypes )
    {
        return Toolbox.pickupAcceptableConstructor( src_constrs,
						    parameterTypes );
    }

    /**
     * Returns an array containing <code>OJClass</code> objects
     * representing all the <em>public</em> classes and interfaces
     * that are members of the class represented by this
     * <code>OJClass</code> object.  This includes public class and
     * interface members inherited from superclasses and public class
     * and interface members declared by the class.  This method
     * returns an array of length 0 if this <code>OJClass</code>
     * object has no public member classes or interfaces.  This method
     * also returns an array of length 0 if this <code>OJClass</code>
     * object represents a primitive type, an array class, or void.
     *
     */
    public OJClass[] getClasses() {
        return removeTheNonPublics( getAllClasses() );
    }

    /**
     * Returns an array containing <code>OJField</code> objects
     * reflecting all the accessible <em>public</em> fields of the
     * class or interface represented by this <code>OJClass</code>
     * object.  The elements in the array returned are not sorted and
     * are not in any particular order.  This method returns an array
     * of length 0 if the class or interface has no accessible public
     * fields, or if it represents an array class, a primitive type,
     * or void.
     *
     * <p> Specifically, if this <code>OJClass</code> object
     * represents a class, this method returns the public fields of
     * this class and of all its superclasses.  If this
     * <code>OJClass</code> object represents an interface, this
     * method returns the fields of this interface and of all its
     * superinterfaces.
     *
     * <p> The implicit length field for array classs is reflected by this
     * method.
     *
     * @see openjava.mop.OJField
     */
    public OJField[] getFields() {
        return removeTheNonPublics( getAllFields() );
    }

    /**
     * Returns an array containing <code>OJMethod</code> objects
     * reflecting all the <em>public</em> member methods of the class
     * or interface represented by this <code>OJClass</code> object,
     * including those declared by the class or interface and and
     * those inherited from superclasses and superinterfaces.  The
     * elements in the array returned are not sorted and are not in
     * any particular order.  This method returns an array of length 0
     * if this <code>OJClass</code> object represents a class or
     * interface that has no public member methods, or if this
     * <code>OJClass</code> object represents an array class, primitive
     * type, or void.
     *
     * @see       openjava.mop.OJMethod
     */
    public OJMethod[] getMethods() {
        return removeTheNonPublics( getAllMethods() );
    }

    /**
     * Returns an array containing <code>OJConstructor</code> objects
     * reflecting all the <em>public</em> constructors of the class
     * represented by this <code>OJClass</code> object.  An array of
     * length 0 is returned if the class has no public constructors,
     * or if the class is an array class, or if the class reflects a
     * primitive type or void.
     *
     * @see openjava.mop.OJConstructor
     */
    public OJConstructor[] getConstructors() {
        return removeTheNonPublics( getDeclaredConstructors() );
    }

    /**
     * Returns a <code>OJField</code> object that reflects the
     * specified <em>public</em> member field of the class or
     * interface represented by this <code>OJClass</code> object. The
     * <code>name</code> parameter is a <code>String</code> specifying
     * the simple name of the desired field.
     *
     * @exception NoSuchMemberException if a field with the specified name is
     *              not found.
     * @see openjava.mop.OJField
     */
    public OJField getField( String name )
  	throws NoSuchMemberException
    {
	OJField field = pickupField( getFields(), name );
	if (field != null)  return field;
        throw new NoSuchMemberException( name );
    }

    /**
     * Returns a <code>OJMethod</code> object that reflects the
     * specified public member method of the class or interface
     * represented by this <code>OJClass</code> object. The
     * <code>name</code> parameter is a <code>String</code> specifying
     * the simple name the desired method. The
     * <code>parameterTypes</code> parameter is an array of
     * <code>OJClass</code> objects that identify the method's formal
     * parameter types, in declared order. If
     * <code>parameterTypes</code> is <code>null</code>, it is treated
     * as if it were an empty array.
     *
     * @exception NoSuchMemberException if a matching method is not found
     *            or if then name is "&lt;init>"or "&lt;clinit>".
     * @see openjava.mop.OJMethod
     */
    public OJMethod getMethod( String name, OJClass[] parameterTypes )
  	throws NoSuchMemberException
    {
        OJMethod method = pickupMethod( getMethods(), name, parameterTypes );
	if (method != null)  return method;
	Signature sign = new Signature( name, parameterTypes );
	throw new NoSuchMemberException( sign.toString() );
    }

    /**
     * Returns a <code>OJConstructor</code> object that reflects the
     * specified public constructor of the class represented by this
     * <code>OJClass</code> object. The <code>parameterTypes</code>
     * parameter is an array of <code>OJClass</code> objects that
     * identify the constructor's formal parameter types, in declared
     * order.
     *
     * <p> The constructor to reflect is the public constructor of the
     * class represented by this <code>OJClass</code> object whose
     * formal parameter types match those specified by
     * <code>parameterTypes</code>.
     *
     * @exception NoSuchMemberException if a matching method is not found.
     * @see openjava.mop.OJConstructor
     */
    public OJConstructor getConstructor( OJClass[] parameterTypes )
  	throws NoSuchMemberException
    {
        OJConstructor constr = pickupConstructor( getConstructors(),
						  parameterTypes );
	if (constr != null)  return constr;
	Signature sign = new Signature( parameterTypes );
	throw new NoSuchMemberException( sign.toString() );
    }

    /**
     * Returns an array containing <code>OJClass</code> objects
     * representing all the classes and interfaces which are members
     * of the class represented by this <code>OJClass</code> object,
     * accessible from the situation represented by the given
     * <code>OJClass</code> object.  This includes class and interface
     * members inherited from superclasses and declared class and
     * interface members accessible from the given situation.  This
     * method returns an array of length 0 if this
     * <code>OJClass</code> object has no public member classes or
     * interfaces.  This method also returns an array of length 0 if
     * this <code>OJClass</code> object represents a primitive type,
     * an array class, or void.
     *
     * <p>The accessiblity depends on the package of the class,
     * modifiers of each members, and the package of the situation.
     *
     */
    public final OJClass[] getClasses( OJClass situation ) {
        if (this == situation)  return getAllClasses();
	if (isInSamePackage( situation )) {
	    return removeThePrivates( getAllClasses() );
	} else if (this.isAssignableFrom( situation )) {
	    return getInheritableClasses( situation );
	}
        return removeTheNonPublics( getAllClasses() );
    }

    /**
     * Returns an array containing <code>OJField</code> objects
     * reflecting all the fields of the class or interface represented
     * by this <code>OJClass</code> object, accessible from the
     * situation represented by the given <code>OJClass</code> object.
     * The elements in the array returned are not sorted and
     * are not in any particular order.  This method returns an array
     * of length 0 if the class or interface has no accessible public
     * fields, or if it represents an array class, a primitive type,
     * or void.
     *
     * <p> Specifically, if this <code>OJClass</code> object
     * represents a class, this method returns the public fields of
     * this class and of all its superclasses.  If this
     * <code>OJClass</code> object represents an interface, this
     * method returns the fields of this interface and of all its
     * superinterfaces.
     *
     * <p>The accessiblity depends on the package of the class,
     * modifiers of each members, and the package of the situation.
     *
     * <p> The implicit length field for array classs is reflected by this
     * method.
     *
     * @see openjava.mop.OJField
     */
    public final OJField[] getFields( OJClass situation ) {
        if (this == situation)  return getAllFields();
	if (isInSamePackage( situation )) {
	    return removeThePrivates( getAllFields() );
	} else if (this.isAssignableFrom( situation )) {
	    return getInheritableFields( situation );
	}
        return removeTheNonPublics( getAllFields() );
    }

    /**
     * Returns an array containing <code>OJMethod</code> objects
     * reflecting all the member methods of the class or interface
     * represented by this <code>OJClass</code> object, accesible from
     * the situation represented by the given <code>OJClass</code>
     * object.  Returned methods include those declared by the class
     * or interface and and those inherited from superclasses and
     * superinterfaces.  The elements in the array returned are not
     * sorted and are not in any particular order.  This method
     * returns an array of length 0 if this <code>OJClass</code>
     * object represents a class or interface that has no public
     * member methods, or if this <code>OJClass</code> object
     * represents an array class, primitive type, or void.
     *
     * <p>The accessiblity depends on the package of the class,
     * modifiers of each members, and the package of the situation.
     *
     * @see openjava.mop.OJMethod
     */
    public final OJMethod[] getMethods( OJClass situation ) {
        if (this == situation)  return getAllMethods();
	if (isInSamePackage( situation )) {
	    return removeThePrivates( getAllMethods() );
	} else if (this.isAssignableFrom( situation )) {
	    return getInheritableMethods( situation );
	}
        return removeTheNonPublics( getAllMethods() );
    }

    /**
     * Returns an array containing <code>OJConstructor</code> objects
     * reflecting all the constructors of the class represented by
     * this <code>OJClass</code> object, accesible from the situation
     * represented by the given <code>OJClass</code> object.  An array
     * of length 0 is returned if the class has no public
     * constructors, or if the class is an array class, or if the
     * class reflects a primitive type or void.
     *
     * <p>The accessiblity depends on the package of the class,
     * modifiers of each members, and the package of the situation.
     *
     * @see openjava.mop.OJConstructor
     */
    public final OJConstructor[] getConstructors( OJClass situation ) {
        if (this == situation)  return getDeclaredConstructors();
	if (isInSamePackage( situation )) {
	    return removeThePrivates( getDeclaredConstructors() );
	} else if (this.isAssignableFrom( situation )) {
	    return getInheritableConstructors( situation );
	}
        return removeTheNonPublics( getDeclaredConstructors() );
    }

    /**
     * Returns a <code>OJField</code> object that reflects the
     * specified member field accesible from the situation represented
     * by the given <code>OJClass</code> object.  The
     * <code>name</code> parameter is a <code>String</code> specifying
     * the simple name of the desired field.
     *
     * @exception NoSuchMemberException if a field with the specified name is
     *              not found.
     * @see openjava.mop.OJField
     */
    public OJField getField( String name, OJClass situation )
  	throws NoSuchMemberException
    {
	OJField field = pickupField( getFields( situation ), name );
	if (field != null)  return field;
        throw new NoSuchMemberException( name );
    }

    /**
     * Returns a <code>OJMethod</code> object that reflects the
     * specified member method accesible from the situation
     * represented by the given <code>OJClass</code> object.  The
     * <code>name</code> parameter is a <code>String</code> specifying
     * the simple name the desired method. The
     * <code>parameterTypes</code> parameter is an array of
     * <code>OJClass</code> objects that identify the method's formal
     * parameter types, in declared order. If
     * <code>parameterTypes</code> is <code>null</code>, it is treated
     * as if it were an empty array.
     *
     * @exception NoSuchMemberException if a matching method is not found.
     * @see openjava.mop.OJMethod
     */
    public OJMethod getMethod( String name, OJClass[] parameterTypes,
			       OJClass situation )
  	throws NoSuchMemberException
    {
	OJMethod method
	    = pickupMethod( getMethods( situation ), name, parameterTypes );
	if (method != null)  return method;
	Signature sign = new Signature( name, parameterTypes );
	throw new NoSuchMemberException( sign.toString() );
    }

    /**
     * Returns a <code>OJConstructor</code> object that reflects the
     * specified constructor accesible from the situation represented
     * by the given <code>OJClass</code> object.  The
     * <code>parameterTypes</code> parameter is an array of
     * <code>OJClass</code> objects that identify the constructor's
     * formal parameter types, in declared order.
     *
     * <p> The constructor to reflect is the constructor of the
     * class represented by this <code>OJClass</code> object whose
     * formal parameter types match those specified by
     * <code>parameterTypes</code>.
     *
     * @exception NoSuchMemberException if a matching method is not found.
     * @see openjava.mop.OJConstructor
     */
    public OJConstructor getConstructor( OJClass[] parameterTypes,
					 OJClass situation )
  	throws NoSuchMemberException
    {
	OJConstructor constr
	    = pickupConstructor( getConstructors( situation ),
				 parameterTypes );
	if (constr != null)  return constr;
	Signature sign = new Signature( parameterTypes );
	throw new NoSuchMemberException( sign.toString() );
    }

    /**
     * Use <code>c.getField(name,c)</code>
     * @deprecated
     * @see #getField(String,OJClass)
     */
    public final OJField getAllField( String name )
  	throws NoSuchMemberException
    {
	OJField field = pickupField( getAllFields(), name );
	if (field != null)  return field;
        throw new NoSuchMemberException( name );
    }

    /**
     * @deprecated
     */
    public final OJMethod[] getAllMethods( String name ) {
	return pickupMethodsByName( getAllMethods(), name );
    }

    /**
     * Use <code>c.getMethod(name,ptypes,c)</code>
     * @deprecated
     * @see #getMethod(String,OJClass[],OJClass)
     */
    public final OJMethod getAllMethod( String name, OJClass[] parameterTypes )
  	throws NoSuchMemberException
    {
	OJMethod method
	    = pickupMethod( getAllMethods(), name, parameterTypes );
	if (method != null)  return method;
	Signature sign = new Signature( name, parameterTypes );
	throw new NoSuchMemberException( sign.toString() );
    }

    /** Can be overriden */
    public OJMethod getAcceptableMethod( String name, OJClass[] parameterTypes,
					 OJClass situation )
  	throws NoSuchMemberException
    {
        OJMethod method = pickupAcceptableMethod( getMethods( situation ),
						  name, parameterTypes );
	if (method != null)  return method;
	Signature sign = new Signature( name, parameterTypes );
	throw new NoSuchMemberException( sign.toString() );
    }

    /** Can be overriden */
    public OJConstructor getAcceptableConstructor( OJClass[] parameterTypes,
						   OJClass situation )
  	throws NoSuchMemberException
    {
        OJConstructor constr = pickupAcceptableConstructor(
	    getConstructors( situation ), parameterTypes );
	if (constr != null)  return constr;
	Signature sign = new Signature( parameterTypes );
	throw new NoSuchMemberException( sign.toString() );
    }

    /**
     * Returns an array of <code>OJClass</code> objects reflecting all
     * the classes and interfaces declared as members of the class
     * represented by this <code>OJClass</code> object. This includes
     * public, protected, default (package) access, and private
     * classes and interfaces declared by the class, but excludes
     * inherited classes and interfaces.  This method returns an array
     * of length 0 if the class declares no classes or interfaces as
     * members, or if this <code>OJClass</code> object represents a
     * primitive type, an array class, or void.
     *
     * <p>This method may be overriden to provide proper information
     * in the extended language.
     */
    public OJClass[] getDeclaredClasses() {
	return substance.getDeclaredClasses();
    }

    /**
     * Returns an array of <code>OJField</code> objects reflecting all
     * the fields declared by the class or interface represented by
     * this <code>OJClass</code> object. This includes public,
     * protected, default (package) access, and private fields, but
     * excludes inherited fields.  The elements in the array returned
     * are not sorted and are not in any particular order.  This
     * method returns an array of length 0 if the class or interface
     * declares no fields, or if this <code>OJClass</code> object
     * represents a primitive type, an array class, or void.
     *
     * <p>This method may be overriden to provide proper information
     * in the extended language.
     *
     * @see openjava.mop.OJField
     */
    public OJField[] getDeclaredFields() {
	return substance.getDeclaredFields();
    }

    /**
     * Returns an array of <code>OJMethod</code> objects reflecting all
     * the methods declared by the class or interface represented by
     * this <code>OJClass</code> object. This includes public,
     * protected, default (package) access, and private methods, but
     * excludes inherited methods.  The elements in the array returned
     * are not sorted and are not in any particular order.  This
     * method returns an array of length 0 if the class or interface
     * declares no methods, or if this <code>OJClass</code> object
     * represents a primitive type, an array class, or void.  The
     * class initialization method <code>&lt;clinit&gt;</code> is not
     * included in the returned array. If the class declares multiple
     * public member methods with the same parameter types, they are
     * all included in the returned array.
     *
     * <p>This method may be overriden to provide proper information
     * in the extended language.
     *
     * @see openjava.mop.OJMethod
     */
    public OJMethod[] getDeclaredMethods() {
	return substance.getDeclaredMethods();
    }

    /**
     * Returns an array of <code>OJConstructor</code> objects reflecting
     * all the constructors declared by the class represented by this
     * <code>OJClass</code> object. These are public, protected, default
     * (package) access, and private constructors.  The elements in
     * the array returned are not sorted and are not in any particular
     * order.  If the class has a default constructor, it is included
     * in the returned array.  This method returns an array of length
     * 0 if this <code>OJClass</code> object represents an interface, a
     * primitive type, an array class, or void.
     *
     * <p>This method may be overriden to provide proper information
     * in the extended language.
     *
     * @see openjava.mop.OJConstructor
     */
    public OJConstructor[] getDeclaredConstructors() {
        return substance.getDeclaredConstructors();
    }

    /**
     * Returns a <code>OJField</code> object that reflects the specified
     * declared field of the class or interface represented by this
     * <code>OJClass</code> object. The <code>name</code> parameter is a
     * <code>String</code> that specifies the simple name of the
     * desired field.  Note that this method will reflect the
     * <code>length</code> field of an array class.
     *
     * @exception NoSuchMemberException if a field with the specified name is
     *              not found.
     * @see openjava.mop.OJField
     */
    public final OJField
    getDeclaredField( String name )
	throws NoSuchMemberException
    {
	OJField field = pickupField( getDeclaredFields(), name );
	if (field != null)  return field;
        throw new NoSuchMemberException( name );
    }

    /**
     * Returns a <code>OJMethod</code> object that reflects the
     * specified declared method of the class or interface represented
     * by this <code>OJClass</code> object. The <code>name</code>
     * parameter is a <code>String</code> that specifies the simple
     * name of the desired method, and the <code>parameterTypes</code>
     * parameter is an array of <code>OJClass</code> objects that
     * identify the method's formal parameter types, in declared
     * order.  If more than one method with the same parameter types
     * is declared in a class, and one of these methods has a return
     * type that is more specific than any of the others, that method
     * is returned; otherwise one of the methods is chosen
     * arbitrarily.
     *
     * @exception NoSuchMemberException if a matching method is not found.
     * @see openjava.mop.OJMethod
     */
    public final OJMethod
    getDeclaredMethod( String name, OJClass[] parameterTypes )
  	throws NoSuchMemberException
    {
	OJMethod method
	    = pickupMethod( getDeclaredMethods(), name, parameterTypes );
	if (method != null)  return method;
	Signature sign = new Signature( name, parameterTypes );
	throw new NoSuchMemberException( sign.toString() );
    }

    /**
     * Returns a <code>OJConstructor</code> object that reflects the
     * specified constructor of the class or interface represented by
     * this <code>OJClass</code> object.  The
     * <code>parameterTypes</code> parameter is an array of
     * <code>OJClass</code> objects that identify the constructor's
     * formal parameter types, in declared order.
     *
     * @exception NoSuchMemberException if a matching method is not found.
     * @see openjava.mop.OJConstructor
     */
    public final OJConstructor
    getDeclaredConstructor( OJClass[] parameterTypes )
  	throws NoSuchMemberException
    {
	OJConstructor constr
	    = pickupConstructor( getDeclaredConstructors(), parameterTypes );
	if (constr != null)  return constr;
	Signature sign = new Signature( parameterTypes );
	throw new NoSuchMemberException( sign.toString() );
    }

    public InputStream getResourceAsStream( String name )
  	throws CannotInspectException
    {
	return substance.getResourceAsStream( name );
    }

    public java.net.URL getResource( String name )
  	throws CannotInspectException
    {
	return substance.getResource( name );
    }

    /* -- the followings do not exist in regular Java -- */

    /**
     * Generate a copy of this class object with the specified name.
     *
     * @param qname  a qualified name for the new copy.
     */
    public OJClass makeCopy( String qname ) throws MOPException {
	DebugOut.println( "makeCopy() of " + getName() +
			 " with a new name: " + qname );
	try {
	    ClassDeclaration org = getSourceCode();
	    ClassDeclaration copy = (ClassDeclaration) org.makeRecursiveCopy();
	    String pack = Environment.toPackageName( qname );
	    String sname = Environment.toSimpleName( qname );
	    copy.setName( sname );
	    copy.accept( new TypeNameQualifier( getEnvironment(), sname ) );
	    FileEnvironment env
		= new FileEnvironment( OJSystem.env, pack, sname );
	    return new OJClass( env, null, copy );
	} catch (CannotAlterException ex1) {
	    return this;
	} catch (ParseTreeException ex2) {
	    throw new MOPException( ex2 );
	}
    }

    public boolean isExecutable() {
	return substance.isExecutable();
    }

    public boolean isAlterable() {
	return substance.isAlterable();
    }

    public Class getByteCode() throws CannotExecuteException {
	return substance.getByteCode();
    }

    public ClassDeclaration getSourceCode() throws CannotAlterException {
        return substance.getSourceCode();
    }

    public Class getCompatibleJavaClass() {
	return substance.getCompatibleJavaClass();
    }

    public Signature signature() {
	return new Signature( this );
    }

    /* -- inner use only -- */

    void setDeclaringClass( OJClass parent ) throws CannotAlterException {
	substance.setDeclaringClass( parent );
    }

    /**
     * Waits a callee-side translation on another class metaobject
     * to be done.
     *
     * @param  clazz  a class metaobject to wait
     */
    public final void waitTranslation( OJClass clazz )
        throws MOPException
    {
        if (! OJSystem.underConstruction.containsKey( clazz ))  return;

	synchronized (OJSystem.waitingPool) {
	    if (OJSystem.waitingPool.contains( clazz )) {
		System.err.println( "a dead lock detected between " +
				   getName() + " and " + clazz.getName() );
		return;
	    }
	    OJSystem.waitingPool.addElement( this );
	}

	OJSystem.waited = clazz;
	Object lock = OJSystem.orderingLock;
	try {
	    synchronized (lock) {
	        synchronized (this) {
		    this.notifyAll();
		}
	        lock.wait();
	    }
	} catch ( InterruptedException e ) {
	    throw new MOPException( e.toString() );
	} finally {
	    OJSystem.waitingPool.removeElement( this );
	}
    }

    /* -- Modifications -- */

    /** not implemented yet */
    protected String setName( String simple_name )
        throws CannotAlterException
    {
        throw new CannotAlterException( "not implemented" );
    }

    protected OJClass setSuperclass( OJClass clazz )
        throws CannotAlterException
    {
	ClassDeclaration d = getSourceCode();
	if (isInterface()) {
	    throw new CannotAlterException(
		"cannot set a superclass of interface" );
	}
	OJClass result = getSuperclass();
	d.setBaseclass( TypeName.forOJClass( clazz ) );
	return result;
    }

    protected OJClass[] setInterfaces( OJClass[] classes )
        throws CannotAlterException
    {
	ClassDeclaration d = getSourceCode();
	OJClass[] result = getInterfaces();
	if (isInterface()) {
	    d.setBaseclasses( Toolbox.TNsForOJClasses( classes ) );
	} else {
	    d.setInterfaces( Toolbox.TNsForOJClasses( classes ) );
	}
	return result;
    }

    protected void addInterface( OJClass clazz )
        throws CannotAlterException
    {
        OJClass[] org = getInterfaces();
	OJClass[] result = new OJClass[org.length + 1];
	System.arraycopy( org, 0, result, 0, org.length );
	result[org.length] = clazz;
	setInterfaces( result );
    }

    public /*was: protected*/ OJClass addClass( OJClass clazz )
	throws CannotAlterException
    {
        return substance.addClass( clazz );
    }

    protected OJClass removeClass( OJClass clazz )
	throws CannotAlterException
    {
        return substance.removeClass( clazz );
    }

    public OJField addField( OJField field )
	throws CannotAlterException
    {
        return substance.addField( field );
    }

    public OJField removeField( OJField field )
	throws CannotAlterException
    {
        return substance.removeField( field );
    }

    protected OJMethod addMethod( OJMethod method )
	throws CannotAlterException
    {
        return substance.addMethod( method );
    }

    protected OJMethod removeMethod( OJMethod method )
	throws CannotAlterException
    {
        return substance.removeMethod( method );
    }

    protected OJConstructor addConstructor( OJConstructor constr )
	throws CannotAlterException
    {
        return substance.addConstructor( constr );
    }

    protected OJConstructor removeConstructor( OJConstructor constr )
	throws CannotAlterException
    {
        return substance.removeConstructor( constr );
    }

    /* -- Translation (overridable) -- */

    public void translateDefinition() throws MOPException {
        ;
    }

    /* */

    public ClassDeclaration translateDefinition( Environment env,
						 ClassDeclaration decl )
	throws MOPException
    {
	OJClass base = getSuperclass();
	if (base != null)  waitTranslation( base );
	OJClass[] faces = getInterfaces();
	for (int i = 0; i < faces.length; ++i) {
	    waitTranslation( faces[i] );
	}
        translateDefinition();
	return decl;
    }

    public Expression expandFieldRead(
	Environment env,
	FieldAccess expr )
    {
	return expr;
    }

    public Expression expandFieldWrite(
	Environment env,
	AssignmentExpression expr )
    {
	return expr;
    }

    public Expression expandMethodCall(
	Environment env,
	MethodCall expr )
    {
	return expr;
    }

    public TypeName expandTypeName(
	Environment env,
	TypeName expr )
    {
	if (isArray()) {
	    return getComponentType().expandTypeName( env, expr );
	}
	return expr;
    }

    public Expression expandAllocation(
	Environment env,
	AllocationExpression expr )
    {
	return expr;
    }

    public Expression expandArrayAllocation(
	Environment env,
	ArrayAllocationExpression expr )
    {
	if (isArray()) {
	    return getComponentType().expandArrayAllocation( env, expr );
	}
	return expr;
    }

    public Expression expandArrayAccess(
	Environment env,
	ArrayAccess expr )
    {
	if (isArray()) {
	    return getComponentType().expandArrayAccess( env, expr );
	}
	return expr;
    }

    public Expression expandAssignmentExpression(
	Environment env,
	AssignmentExpression expr )
    {
	if (isArray()) {
	    return getComponentType().expandAssignmentExpression( env, expr );
	}
	return expr;
    }

    public Expression expandExpression(
	Environment env,
	Expression expr )
    {
	if (isArray()) {
	    return getComponentType().expandExpression( env, expr );
	}
	return expr;
    }

    public Statement expandVariableDeclaration(
	Environment env,
	VariableDeclaration decl )
    {
	if (isArray()) {
	    return getComponentType().expandVariableDeclaration(env, decl);
	}
	return decl;
    }

    public Expression expandCastExpression(
	Environment env,
	CastExpression decl )
    {
	if (isArray()) {
	    return getComponentType().expandCastExpression(env, decl);
	}
	return decl;
    }

    public Expression expandCastedExpression(
	Environment env,
	CastExpression decl )
    {
	if (isArray()) {
	    return getComponentType().expandCastedExpression(env, decl);
	}
	return decl;
    }

    /* -- error handling -- */

    public OJField resolveException( NoSuchMemberException e, String name )
	throws NoSuchMemberException
    {
	System.err.println( "no such " + new Signature( name ) +
			   " in " + toString() );
	throw e;
    }

    public OJMethod resolveException( NoSuchMemberException e,
				 String name, OJClass[] argtypes )
	throws NoSuchMemberException
    {
	System.err.println( "no such " + new Signature( name, argtypes ) +
			   " in " + toString() );
	throw e;
    }

    /* -- syntax extensions -- */

    public static boolean isRegisteredKeyword( String keyword ) {
	return false;
    }

    public static SyntaxRule getDeclSuffixRule( String keyword ) {
	return null;
    }

    public static SyntaxRule getTypeSuffixRule( String keyword ) {
	return null;
    }

    public static boolean isRegisteredModifier( String keyword ) {
	return false;
    }

    /* -- persistant metalevel information */

    public final String getMetaInfo( String key ) {
	return substance.getMetaInfo( key );
    }

    public final Enumeration getMetaInfoKeys() {
	return substance.getMetaInfoKeys();
    }

    public final Enumeration getMetaInfoElements() {
	return substance.getMetaInfoElements();
    }

    protected final String putMetaInfo( String key, String value )
	throws CannotAlterException
    {
	return substance.putMetaInfo( key, value );
    }

    /** inner use only */
    public final void writeMetaInfo( Writer out ) throws IOException {
	substance.writeMetaInfo( out );
    }

}


/**
 * The abstract class <code>OJClassImp</code> provides an interface to
 * an implementation of OJClass.
 */
abstract class OJClassImp
{
    public abstract String toString();
    abstract ClassEnvironment getEnvironment();
    abstract Object newInstance()
	throws InstantiationException, IllegalAccessException,
	    CannotExecuteException;
    abstract boolean isInterface();
    abstract boolean isArray();
    abstract boolean isPrimitive();

    abstract String getName();

    abstract ClassLoader getClassLoader()
	throws CannotInspectException;

    abstract OJClass getSuperclass();
    abstract OJClass[] getInterfaces();
    abstract OJClass getComponentType();
    abstract OJModifier getModifiers();
    abstract ParseTree getSuffix( String keyword );
    abstract Object[] getSigners()
	throws CannotExecuteException;
    abstract OJClass getDeclaringClass();

    abstract OJClass[] getDeclaredClasses();
    abstract OJField[] getDeclaredFields();
    abstract OJMethod[] getDeclaredMethods();
    abstract OJConstructor[] getDeclaredConstructors();

    abstract InputStream getResourceAsStream( String name )
	throws CannotInspectException;
    abstract java.net.URL getResource( String name )
	throws CannotInspectException;

    abstract boolean isExecutable();
    abstract boolean isAlterable();
    abstract Class getByteCode()
	throws CannotExecuteException;
    abstract ClassDeclaration getSourceCode()
	throws CannotAlterException;
    abstract Class getCompatibleJavaClass();

    abstract void setDeclaringClass( OJClass parent )
	throws CannotAlterException;

    abstract OJClass addClass( OJClass clazz )
	throws CannotAlterException;
    abstract OJClass removeClass( OJClass clazz )
	throws CannotAlterException;
    abstract OJField addField( OJField field )
	throws CannotAlterException;
    abstract OJField removeField( OJField field )
	throws CannotAlterException;
    abstract OJMethod addMethod( OJMethod method )
	throws CannotAlterException;
    abstract OJMethod removeMethod( OJMethod method )
	throws CannotAlterException;
    abstract OJConstructor addConstructor( OJConstructor constr )
	throws CannotAlterException;
    abstract OJConstructor removeConstructor( OJConstructor constr )
	throws CannotAlterException;

    abstract String getMetaInfo( String key );
    abstract Enumeration getMetaInfoKeys();
    abstract Enumeration getMetaInfoElements();
    abstract String putMetaInfo( String key, String value )
	throws CannotAlterException;
    abstract void writeMetaInfo( Writer out ) throws IOException;

    final OJClass forNameAnyway( String name ) {
	return Toolbox.forNameAnyway( getEnvironment(), name );
    }

    final OJClass[] arrayForNames( String[] names ) {
	return Toolbox.arrayForNames( getEnvironment(),  names );
    }

    static final OJClass forClass( Class javaclass ) {
	return OJClass.forClass( javaclass );
    }

    static final String nameForJavaClassName( String javaname ) {
	return Toolbox.nameForJavaClassName( javaname );
    }

    static final String nameToJavaClassName( String ojname ) {
	return Toolbox.nameToJavaClassName( ojname );
    }

    static final OJField[] arrayForFields( Field[] fields ) {
	return OJField.arrayForFields( fields );
    }

    static final OJMethod[] arrayForMethods( Method[] methods ) {
	return OJMethod.arrayForMethods( methods );
    }

    static final OJConstructor[]
    arrayForConstructors( Constructor[] constrs ) {
	return OJConstructor.arrayForConstructors( constrs );
    }

}


class OJClassByteCode extends OJClassImp
{
    private Class		javaClass;

    private MetaInfo		metainfo;

    OJClassByteCode( Class java_class, MetaInfo metainfo ) {
	this.javaClass = java_class;
	this.metainfo = metainfo;
    }

    ClassEnvironment getEnvironment() {
	int last = getName().lastIndexOf( '.' );
	String pack = (last == -1) ? null : getName().substring( 0, last );
	String name = Environment.toSimpleName( getName() );
	FileEnvironment fenv = new FileEnvironment( OJSystem.env, pack, name );
        return new ClassEnvironment( fenv, name );
    }

    public String toString() {
	return (isPrimitive() ? getName() : "class " + getName());
    }

    Object newInstance()
  	throws InstantiationException, IllegalAccessException
    {
        return javaClass.newInstance();
    }

    boolean isInterface() {
        return javaClass.isInterface();
    }

    boolean isArray() {
	return javaClass.isArray();
    }

    boolean isPrimitive() {
	return javaClass.isPrimitive();
    }

    String getName() {
        return nameForJavaClassName( javaClass.getName() );
    }

    ClassLoader getClassLoader() throws CannotInspectException {
        return javaClass.getClassLoader();
    }

    OJClass getSuperclass() {
	Class base = javaClass.getSuperclass();
        return ((base == null) ? null : forClass( base ));
    }

    OJClass[] getInterfaces() {
	return OJClass.arrayForClasses( javaClass.getInterfaces() );
    }

    OJClass getComponentType() {
	Class comp = javaClass.getComponentType();
        return ((comp == null) ? null : forClass( comp ));
    }

    OJModifier getModifiers() {
        return OJModifier.forModifier( javaClass.getModifiers() );
    }

    ParseTree getSuffix( String keyword ) {
        return null;
    }

    Object[] getSigners() throws CannotExecuteException {
        return javaClass.getSigners();
    }

    OJClass getDeclaringClass() {
	Class declarer = javaClass.getDeclaringClass();
        return ((declarer == null) ? null : forClass( declarer ));
    }

    OJClass[] getDeclaredClasses() {
	try {
	    return OJClass.arrayForClasses( javaClass.getDeclaredClasses() );
	} catch (SecurityException e) {
	    System.err.println( e );
	    return new OJClass[0];
	}
    }

    OJField[] getDeclaredFields() {
	try {
	    return arrayForFields( javaClass.getDeclaredFields() );
	} catch (SecurityException e) {
	    System.err.println( e );
	    return new OJField[0];
	}
    }

    OJMethod[] getDeclaredMethods() {
	try {
	    return arrayForMethods( javaClass.getDeclaredMethods() );
	} catch (SecurityException e) {
	    System.err.println( e );
	    return new OJMethod[0];
	}
    }

    OJConstructor[] getDeclaredConstructors() {
	try {
	    return arrayForConstructors( javaClass.getDeclaredConstructors() );
	} catch (SecurityException e) {
	    System.err.println( e );
	    return new OJConstructor[0];
	}
    }

    InputStream getResourceAsStream( String name )
	throws CannotInspectException
    {
        return javaClass.getResourceAsStream( name );
    }

    java.net.URL getResource( String name )
	throws CannotInspectException
    {
        return javaClass.getResource( name );
    }

    /* -- the followings do not exist in regular Java -- */

    boolean isExecutable() {
	return true;
    }

    boolean isAlterable() {
	return false;
    }

    Class getByteCode() throws CannotExecuteException {
        return javaClass;
    }

    ClassDeclaration getSourceCode() throws CannotAlterException {
	throw new CannotAlterException( "getSourceCode()" );
    }

    Class getCompatibleJavaClass() {
	return javaClass;
    }

    void setDeclaringClass( OJClass parent ) throws CannotAlterException {
	throw new CannotAlterException( "setDeclaringClass()" );
    }

    OJClass addClass( OJClass clazz ) throws CannotAlterException {
	throw new CannotAlterException( "addClass()" );
    }

    OJClass removeClass( OJClass clazz ) throws CannotAlterException {
	throw new CannotAlterException( "removeClass()" );
    }

    OJField addField( OJField field ) throws CannotAlterException {
	throw new CannotAlterException( "addField()" );
    }

    OJField removeField( OJField field ) throws CannotAlterException {
	throw new CannotAlterException( "removeField()" );
    }

    OJMethod addMethod( OJMethod method ) throws CannotAlterException {
	throw new CannotAlterException( "addMethod()" );
    }

    OJMethod removeMethod( OJMethod method ) throws CannotAlterException {
	throw new CannotAlterException( "removeMethod()" );
    }

    OJConstructor addConstructor( OJConstructor constr )
	throws CannotAlterException
    {
	throw new CannotAlterException( "addConstructor()" );
    }

    OJConstructor removeConstructor( OJConstructor constr )
	throws CannotAlterException
    {
	throw new CannotAlterException( "removeConstructor()" );
    }

    /* -- persistant metalevel information */

    String getMetaInfo( String key ) {
	return metainfo.get( key );
    }

    Enumeration getMetaInfoKeys() {
	return metainfo.keys();
    }

    Enumeration getMetaInfoElements() {
	return metainfo.elements();
    }

    String putMetaInfo( String key, String value )
	throws CannotAlterException
    {
	throw new CannotAlterException( "putMetaInfo()" );
    }

    void writeMetaInfo( Writer out ) throws IOException {
    }

}


class OJClassSourceCode extends OJClassImp
{
    private OJClass		declarer;
    private ClassDeclaration	definition;
    private ClassEnvironment	env;

    private Vector classes = new Vector();
    private Vector fields = new Vector();
    private Vector methods = new Vector();
    private Vector constrs = new Vector();

    private MetaInfo		metainfo;

    /* -- constructors -- */

    OJClassSourceCode( OJClass holder, Environment outer_env,
		       OJClass declarer, ClassDeclaration ptree )
    {
	this.declarer = declarer;
	this.definition = ptree;
	String qname;
	if (declarer == null) {
	    qname = outer_env.toQualifiedName( definition.getName() );
	    //String pack = outer_env.getPackage();
	    //qname = ((pack == null) ? "" : pack + ".") + definition.getName();
	} else {
	    qname = outer_env.currentClassName() +
		"." + definition.getName();
	}
	this.env = new ClassEnvironment( outer_env, qname );
	metainfo = new MetaInfo( holder.getClass().getName(), qname );

	MemberDeclarationList mdecls = ptree.getBody();
	for (int i = 0, len = mdecls.size(); i < len; ++i) {
	    MemberDeclaration mdecl = mdecls.get( i );
	    if (mdecl instanceof ClassDeclaration) {
		ClassDeclaration d = (ClassDeclaration) mdecl;
		this.env.recordMemberClass( d.getName() );
                try {
		    OJClass clazz
                        = OJClass.forParseTree(this.env, holder, d);
		    this.classes.addElement( clazz );
                } catch (Exception ex) {
                    /***** here should be error-handling */
                    ex.printStackTrace();
                }
	    } else if (mdecl instanceof FieldDeclaration) {
		FieldDeclaration d = (FieldDeclaration) mdecl;
		OJField field
		    = new OJField( this.env, holder, d );
		this.fields.addElement( field );
	    } else if (mdecl instanceof MethodDeclaration) {
		MethodDeclaration d = (MethodDeclaration) mdecl;
		OJMethod method
		    = new OJMethod( this.env, holder, d );
		this.methods.addElement( method );
	    } else if (mdecl instanceof ConstructorDeclaration) {
		ConstructorDeclaration d = (ConstructorDeclaration) mdecl;
		OJConstructor constr
		    = new OJConstructor( this.env, holder, d );
		this.constrs.addElement( constr );
	    } else if (mdecl instanceof MemberInitializer) {
		/***********/;
	    }
	}
    }

    public String toString() {
	return ("class " + getName());
    }

    ClassEnvironment getEnvironment() {
        return env;
    }

    Object newInstance()
  	throws InstantiationException, IllegalAccessException
    {
	throw new InstantiationException( "not compiled yet" );
    }

    boolean isInterface() {
	return definition.isInterface();
    }

    boolean isArray() {
	return false;
    }

    boolean isPrimitive() {
	return false;
    }

    String getName() {
	if (declarer == null) {
	    return env.toQualifiedName( definition.getName() );
	} else {
	    return declarer.getName() + "." + definition.getName();
	}
    }

    ClassLoader getClassLoader() throws CannotInspectException {
	throw new CannotInspectException( "getClassLoader()" );
    }

    OJClass getSuperclass() {
	if (isInterface()) {
	    return null;
	} else {
	    TypeName base = definition.getBaseclass();
	    String basename
		= (base == null) ? "java.lang.Object" : base.toString();
	    return forNameAnyway( basename );
	}
    }

    OJClass[] getInterfaces() {
	TypeName[] types;
	if (isInterface()) {
	    types = definition.getBaseclasses();
	} else {
	    types = definition.getInterfaces();
	}
	String[] names = new String[types.length];
	for (int i = 0; i < names.length; ++i) {
	    names[i] = types[i].toString();
	}
	return arrayForNames( names );
    }

    OJClass getComponentType() {
        return null;
    }

    OJModifier getModifiers() {
	return OJModifier.forParseTree( definition.getModifiers() );
    }

    ParseTree getSuffix( String keyword ) {
        Hashtable table = definition.getSuffixes();
	if (table == null)  return null;
	return (ParseTree) table.get( keyword );
    }

    Object[] getSigners() throws CannotExecuteException {
        throw new CannotExecuteException( "getSigners()" );
    }

    OJClass getDeclaringClass() {
        return declarer;
    }

    OJClass[] getDeclaredClasses() {
	OJClass[] result = new OJClass[classes.size()];
	for (int i = 0; i < result.length; ++i) {
	    result[i] = (OJClass) classes.elementAt( i );
	}
        return result;
    }

    OJField[] getDeclaredFields() {
	OJField[] result = new OJField[fields.size()];
	for (int i = 0; i < result.length; ++i) {
	    result[i] = (OJField) fields.elementAt( i );
	}
        return result;
    }

    OJMethod[] getDeclaredMethods() {
	OJMethod[] result = new OJMethod[methods.size()];
	for (int i = 0; i < result.length; ++i) {
	    result[i] = (OJMethod) methods.elementAt( i );
	}
        return result;
    }

    OJConstructor[] getDeclaredConstructors() {
	OJConstructor[] result = new OJConstructor[constrs.size()];
	for (int i = 0; i < result.length; ++i) {
	    result[i] = (OJConstructor) constrs.elementAt( i );
	}
        return result;
    }

    InputStream getResourceAsStream( String name )
  	throws CannotInspectException
    {
	throw new CannotInspectException( "getResourceAsStream()" );
    }

    java.net.URL getResource( String name )
  	throws CannotInspectException
    {
	throw new CannotInspectException( "getResource()" );
    }

    /* -- the followings do not exist in regular Java -- */

    boolean isExecutable() {
	return false;
    }

    boolean isAlterable() {
	return true;
    }

    Class getByteCode() throws CannotExecuteException {
        throw new CannotExecuteException( "getByteCode()" );
    }

    ClassDeclaration getSourceCode() throws CannotAlterException {
	return definition;
    }

    Class getCompatibleJavaClass() {
        return getSuperclass().getCompatibleJavaClass();
    }

    void setDeclaringClass( OJClass parent ) throws CannotAlterException {
	this.declarer = parent;
    }

    private boolean isUniqueName(String name) {

        Enumeration elemnts = classes.elements();
        while (elemnts.hasMoreElements()) {
            if (((OJClass)elemnts.nextElement()).getName().equals(name)) {
                return false;
            }
        }
        return true;
    }

    OJClass addClass( OJClass clazz ) throws CannotAlterException {
	if (! clazz.isAlterable()) {
	    throw new CannotAlterException( "cannot add by addClass()" );
	}
	OJClass result = clazz;
	/*ClassDeclaration result = clazz.makeCopy();*/
    assert (isUniqueName(result.getName())) : "Cannot add duplicate class name " + result.getName() ;
	this.classes.addElement( result );
	ClassDeclaration cdecl = result.getSourceCode();
	MemberDeclarationList memdecls = getSourceCode().getBody();
	memdecls.add( cdecl );
	return result;
    }

    OJClass removeClass( OJClass clazz ) throws CannotAlterException {
	if (! clazz.isAlterable()) {
	    throw new CannotAlterException( "cannot remove by removeClass()" );
	}
	if (! classes.removeElement( clazz ))  return null;
	OJClass result = clazz;
	ClassDeclaration cdecl = result.getSourceCode();
	MemberDeclarationList memdecls = getSourceCode().getBody();
	for (int i = 0; i < memdecls.size(); ++i) {
	    if (memdecls.get( i ) == cdecl)  memdecls.remove( i-- );
	}
	return result;
    }

    OJField addField( OJField field ) throws CannotAlterException {
	if (! field.isAlterable()) {
	    throw new CannotAlterException( "cannot add by addField()" );
	}
	OJField result = field;
	/*FieldDeclaration result = field.makeCopy();*/
	this.fields.addElement( result );
	//result.set
	FieldDeclaration fdecl = result.getSourceCode();
	MemberDeclarationList memdecls = getSourceCode().getBody();
	memdecls.add( fdecl );
	return result;
    }

    OJField removeField( OJField field ) throws CannotAlterException {
	if (! field.isAlterable()) {
	    throw new CannotAlterException( "cannot remove by removeField()" );
	}
	if (! fields.removeElement( field ))  return null;
	OJField result = field;
	FieldDeclaration fdecl = result.getSourceCode();
	MemberDeclarationList memdecls = getSourceCode().getBody();
	for (int i = 0; i < memdecls.size(); ++i) {
	    if (memdecls.get( i ) == fdecl)  memdecls.remove( i-- );
	}
	return result;
    }

    OJMethod addMethod( OJMethod method ) throws CannotAlterException {
	if (! method.isAlterable()) {
	    throw new CannotAlterException( "cannot add by addMethod()" );
	}
	OJMethod result = method;
	/*MethodDeclaration result = field.makeCopy();*/
	this.methods.addElement( result );
	MethodDeclaration mdecl = result.getSourceCode();
	MemberDeclarationList memdecls = getSourceCode().getBody();
	memdecls.add( mdecl );
	return result;
    }

    OJMethod removeMethod( OJMethod method ) throws CannotAlterException {
	if (! method.isAlterable()) {
	    throw new CannotAlterException(
		"cannot remove by removeMethod()" );
	}
	if (! methods.removeElement( method ))  return null;
	OJMethod result = method;
	MethodDeclaration fdecl = result.getSourceCode();
	MemberDeclarationList memdecls = getSourceCode().getBody();
	for (int i = 0; i < memdecls.size(); ++i) {
	    if (memdecls.get( i ) == fdecl)  memdecls.remove( i-- );
	}
	return result;
    }

    OJConstructor addConstructor( OJConstructor constr )
	throws CannotAlterException
    {
	if (! constr.isAlterable()) {
	    throw new CannotAlterException( "cannot add by addConstructor()" );
	}
	OJConstructor result = constr;
	/*ConstructorDeclaration result = constr.makeCopy();*/
	this.constrs.addElement( result );
	ConstructorDeclaration mdecl = result.getSourceCode();
	MemberDeclarationList memdecls = getSourceCode().getBody();
	memdecls.add( mdecl );
	return result;
    }

    OJConstructor removeConstructor( OJConstructor constr )
	throws CannotAlterException
    {
	if (! constr.isAlterable()) {
	    throw new CannotAlterException(
		"cannot remove by removeConstructor()" );
	}
	if (! constrs.removeElement( constr ))  return null;
	OJConstructor result = constr;
	ConstructorDeclaration fdecl = result.getSourceCode();
	MemberDeclarationList memdecls = getSourceCode().getBody();
	for (int i = 0; i < memdecls.size(); ++i) {
	    if (memdecls.get( i ) == fdecl)  memdecls.remove( i-- );
	}
	return result;
    }

    /* -- persistant metalevel information */

    String getMetaInfo( String key ) {
	return metainfo.get( key );
    }

    Enumeration getMetaInfoKeys() {
	return metainfo.keys();
    }

    Enumeration getMetaInfoElements() {
	return metainfo.elements();
    }

    String putMetaInfo( String key, String value )
	throws CannotAlterException
    {
	return metainfo.put( key, value );
    }

    void writeMetaInfo( Writer out ) throws IOException {
	metainfo.write( out );
    }

}


class OJClassArray extends OJClassImp
{
    private OJClass		componentType;
    private OJClass[]		classes = new OJClass[0];
    private OJMethod[]		methods = new OJMethod[0];
    private OJConstructor[]	constrs = new OJConstructor[0];
    private Vector		fields = new Vector();

    private MetaInfo	metainfo;

    /* -- constructors -- */

    OJClassArray(OJClass componentType) {
	this.componentType = componentType;
	fields.addElement( makeLengthField() );
	this.metainfo = new MetaInfo( componentType.getName() + "[]" );
    }

    private final OJField makeLengthField() {
	OJModifier modif
	    = new OJModifier( OJModifier.PUBLIC | OJModifier.FINAL );
	OJClass type = OJClass.forClass( int . class );
	/* exactly not conmponent type */
	return new OJField( componentType, modif, type, "length" );
    }

    ClassEnvironment getEnvironment() {
	return (ClassEnvironment) componentType.getEnvironment();
    }

    public String toString() {
	return ("class " + componentType.getName() + "[]");
    }

    Object newInstance()
  	throws InstantiationException, IllegalAccessException
    {
	throw new InstantiationException( "not compiled yet" );
    }

    boolean isInterface() {
	return false;
    }

    boolean isArray() {
	return true;
    }

    boolean isPrimitive() {
	return false;
    }

    String getName() {
	return (componentType.getName() + "[]");
    }

    ClassLoader getClassLoader() throws CannotInspectException {
	throw new CannotInspectException( "getClassLoader()" );
    }

    OJClass getSuperclass() {
	return OJClass.forClass(Object[].class.getSuperclass());
    }

    OJClass[] getInterfaces() {
	return OJClass.arrayForClasses(Object[].class.getInterfaces());
    }

    OJClass getComponentType() {
        return componentType;
    }

    OJModifier getModifiers() {
        return OJModifier.forModifier(Object[].class.getModifiers() );
    }

    ParseTree getSuffix(String keyword) {
        return null;
    }

    Object[] getSigners() throws CannotExecuteException {
        throw new CannotExecuteException( "getSigners()" );
    }

    OJClass getDeclaringClass() {
        return null;
    }

    OJClass[] getDeclaredClasses() {
        return classes;
    }

    OJField[] getDeclaredFields() {
	OJField[] result = new OJField[fields.size()];
	for (int i = 0; i < result.length; ++i) {
	    result[i] = (OJField) fields.elementAt( i );
	}
	return result;
    }

    OJMethod[] getDeclaredMethods() {
        return methods;
    }

    OJConstructor[] getDeclaredConstructors() {
        return constrs;
    }

    InputStream getResourceAsStream( String name )
  	throws CannotInspectException
    {
	throw new CannotInspectException( "getResourceAsStream()" );
    }

    java.net.URL getResource( String name )
  	throws CannotInspectException
    {
	throw new CannotInspectException( "getResource()" );
    }

    /* -- the followings do not exist in regular Java -- */

    boolean isExecutable() {
	return false;
    }

    boolean isAlterable() {
	return false;
    }

    Class getByteCode() throws CannotExecuteException {
        throw new CannotExecuteException( "getByteCode()" );
    }

    ClassDeclaration getSourceCode() throws CannotAlterException {
	throw new CannotAlterException( "getSourceCode()" );
    }

    Class getCompatibleJavaClass() {
        return getSuperclass().getCompatibleJavaClass();
    }

    void setDeclaringClass( OJClass parent ) throws CannotAlterException {
	throw new CannotAlterException( "setDeclaringClass()" );
    }

    OJClass addClass( OJClass clazz ) throws CannotAlterException {
	throw new CannotAlterException( "addClass()" );
    }

    OJClass removeClass( OJClass clazz ) throws CannotAlterException {
	throw new CannotAlterException( "removeClass()" );
    }

    OJField addField( OJField field ) throws CannotAlterException {
	throw new CannotAlterException( "addField()" );
    }

    OJField removeField( OJField field ) throws CannotAlterException {
	throw new CannotAlterException( "removeField()" );
    }

    OJMethod addMethod( OJMethod method ) throws CannotAlterException {
	throw new CannotAlterException( "addMethod()" );
    }

    OJMethod removeMethod( OJMethod method ) throws CannotAlterException {
	throw new CannotAlterException( "removeMethod()" );
    }

    OJConstructor addConstructor( OJConstructor constr )
	throws CannotAlterException
    {
	throw new CannotAlterException( "addConstructor()" );
    }

    OJConstructor removeConstructor( OJConstructor constr )
	throws CannotAlterException
    {
	throw new CannotAlterException( "removeConstructor()" );
    }

    /* -- persistant metalevel information */

    String getMetaInfo( String key ) {
	return null;
    }

    Enumeration getMetaInfoKeys() {
	return new Vector().elements();
    }

    Enumeration getMetaInfoElements() {
	return new Vector().elements();
    }

    String putMetaInfo( String key, String value )
	throws CannotAlterException
    {
	throw new CannotAlterException( "putMetaInfo()" );
    }

    void writeMetaInfo( Writer out ) throws IOException {
    }

}


class OJClassNull extends OJClassImp
{

    OJClassNull() {}

    ClassEnvironment getEnvironment() {
        return new ClassEnvironment( OJSystem.env, getName() );
    }

    public String toString() {
	return OJSystem.NULLTYPE_NAME;
    }

    Object newInstance()
  	throws InstantiationException, IllegalAccessException
    {
	throw new InstantiationException( "null type" );
    }

    boolean isInterface() {
	return false;
    }

    boolean isArray() {
	return false;
    }

    boolean isPrimitive() {
	return false;
    }

    String getName() {
	return null;
    }

    ClassLoader getClassLoader() throws CannotInspectException {
	throw new CannotInspectException( "getClassLoader()" );
    }

    OJClass getSuperclass() {
	return null;
    }

    OJClass[] getInterfaces() {
	return null;
    }

    OJClass getComponentType() {
        return null;
    }

    OJModifier getModifiers() {
	return null;
    }

    ParseTree getSuffix( String keyword ) {
        return null;
    }

    Object[] getSigners() throws CannotExecuteException {
        throw new CannotExecuteException( "getSigners()" );
    }

    OJClass getDeclaringClass() {
        return null;
    }

    OJClass[] getDeclaredClasses() {
        return null;
    }

    OJField[] getDeclaredFields() {
	return null;
    }

    OJMethod[] getDeclaredMethods() {
        return null;
    }

    OJConstructor[] getDeclaredConstructors() {
        return null;
    }

    InputStream getResourceAsStream( String name )
  	throws CannotInspectException
    {
	throw new CannotInspectException( "getResourceAsStream()" );
    }

    java.net.URL getResource( String name )
  	throws CannotInspectException
    {
	throw new CannotInspectException( "getResource()" );
    }

    /* -- the followings do not exist in regular Java -- */

    boolean isExecutable() {
	return false;
    }

    boolean isAlterable() {
	return false;
    }

    Class getByteCode() throws CannotExecuteException {
        throw new CannotExecuteException( "getByteCode()" );
    }

    ClassDeclaration getSourceCode() throws CannotAlterException {
	throw new CannotAlterException( "getSourceCode()" );
    }

    Class getCompatibleJavaClass() {
        return null;
    }

    void setDeclaringClass( OJClass parent ) throws CannotAlterException {
	throw new CannotAlterException( "setDeclaringClass()" );
    }

    OJClass addClass( OJClass clazz ) throws CannotAlterException {
	throw new CannotAlterException( "addClass()" );
    }

    OJClass removeClass( OJClass clazz ) throws CannotAlterException {
	throw new CannotAlterException( "removeClass()" );
    }

    OJField addField( OJField field ) throws CannotAlterException {
	throw new CannotAlterException( "addField()" );
    }

    OJField removeField( OJField field ) throws CannotAlterException {
	throw new CannotAlterException( "removeField()" );
    }

    OJMethod addMethod( OJMethod method ) throws CannotAlterException {
	throw new CannotAlterException( "addMethod()" );
    }

    OJMethod removeMethod( OJMethod method ) throws CannotAlterException {
	throw new CannotAlterException( "removeMethod()" );
    }

    OJConstructor addConstructor( OJConstructor constr )
	throws CannotAlterException
    {
	throw new CannotAlterException( "addConstructor()" );
    }

    OJConstructor removeConstructor( OJConstructor constr )
	throws CannotAlterException
    {
	throw new CannotAlterException( "removeConstructor()" );
    }

    /* -- persistant metalevel information */

    String getMetaInfo( String key ) {
	return null;
    }

    Enumeration getMetaInfoKeys() {
	return new Vector().elements();
    }

    Enumeration getMetaInfoElements() {
	return new Vector().elements();
    }

    String putMetaInfo( String key, String value )
	throws CannotAlterException
    {
	throw new CannotAlterException( "putMetaInfo()" );
    }

    void writeMetaInfo( Writer out ) throws IOException {
    }
}
