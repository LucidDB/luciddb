/*
 * $Id$
 *
 *
 * Jul 28, 1998 by mich
 */
package openjava.mop;


import openjava.mop.OJClass;
import org.eigenbase.relopt.RelOptSchema;


public abstract class Environment
{

    protected Environment parent;
  
    public Environment() {
        parent = null;
    }
  
    public Environment( Environment e ) {
        parent = e;
    }

    public abstract String toString();

    /**
     * Gets the parent environment.
     **/
    public Environment getParent()
    {
	return parent;
    }

    /**
     * Gets the nearest ancestor which is a {@link ClassEnvironment}, or null.
     **/
    public ClassEnvironment getClassEnvironmentParent()
    {
	if (parent == null) {
	    return null;
	} else {
	    return parent.getClassEnvironmentParent();
	}
    }

    /**
     * Gets the root environment, which is always a {@link GlobalEnvironment}.
     **/
    public GlobalEnvironment getGlobalEnvironment()
    {
	for (Environment e = this;;) {
	    Environment parent = e.getParent();
	    if (parent == null) {
		return (GlobalEnvironment) e;
	    } else {
		e = parent;
	    }
	}
    }

    /**
     * Gets the package name.
     */
    public String getPackage()
    {
        if (parent == null) {
	    System.err.println( "Environment.getPackage() : not specified." );
	    return null;
	}
        return parent.getPackage();
    }
  
    /**
     * Looks a class object up.
     *
     * @param	name		the name of the fully-qualified name of
     *				the class looked for
     */
    public OJClass lookupClass( String name ) {
        if (parent == null) {
	    System.err.println( "Environment.lookupClass() : not specified." );
	    return null;
	}
        return parent.lookupClass( name );
    }

	/**
	 * Looks up a class with a given dimension. For example, <code>
	 * lookupClass("java.lang.String", 2)</code> returns the OJClass
	 * representing the class <code>String[][]</code>.
	 *
	 * @param name      the fully-qualified name of the class
	 * @param dimensionCount the dimensionality of the type
	 * @return a class with the given name and dimensionality
	 */
	public OJClass lookupClass( String name, int dimensionCount ) {
		OJClass clazz = lookupClass(name);
		if (clazz == null) {
			return null;
		}
		for (int i = 0; i < dimensionCount; i++) {
			clazz = OJClass.arrayOf(clazz);
		}
		return clazz;
	}

    /**
     * Records a class object.
     *
     * @param	name		the fully-qualified name of the class
     * @param	clazz		the class object associated with that name
     */
    public abstract void record( String name, OJClass clazz );
  
    /**
     * Looks up a binded type of the given variable or field name.
     *
     * @param	name		the fully-qualified name of the class
     */
    public VariableInfo lookupBind( String name ) {
        if (parent == null)  return null;
        return parent.lookupBind( name );
    }
  
    /**
     * Returns whether a name is a variable.  (In some situations, we cannot
     * use {@link #lookupBind}, because the tree is not yet in a state where we
     * can infer types.)
     **/
    public boolean isBind( String name ) {
		VariableInfo info = lookupBind( name );
		return info != null;
    }

    /**
     * Binds the name of a variable to its class.
     *
     * @param	name		the fully-qualified name of the class
     * @param	clazz		the class object associated with that name
     */
	public void bindVariable(String name, final OJClass clazz) {
		bindVariable(name, new BasicVariableInfo(clazz));
	}

	/**
	 * Binds the name of a variable to information about the variable, including
	 * its class.
	 */
    public abstract void bindVariable( String name, VariableInfo info );

	public interface VariableInfo {
		OJClass getType();
		RelOptSchema getRelOptSchema();
	}

	protected static class BasicVariableInfo implements VariableInfo {
		private OJClass clazz;

		public BasicVariableInfo(OJClass clazz) {
			this.clazz = clazz;
		}

		public OJClass getType() {
			return clazz;
		}

		public RelOptSchema getRelOptSchema() {
			return null;
		}
	}

    /**
     * Obtains the fully-qualified name of the given class name.
     *
     * @param  name  a simple class name or a fully-qualified class name
     * @return  the fully-qualified name of the class
     */
    public String toQualifiedName( String name ) {
	if (parent == null)  return name;
	return parent.toQualifiedName( name );
    }
  
    /**
     * Tests if the given name is a qualified name or not.
     */
    public static boolean isQualifiedName( String name ) {
        return name.indexOf( '.' ) >= 0;
    }
  
    /**
     * Converts a fully-qualified name to the corresponding simple-name.
     *
     * <pre>
     * For example :
     *   toSimpleName( "java.lang.Class" ) returns  "Class".
     * </pre>
     *
     * @return	the given name <I>as is</I> if it is not a qualified name
     */
    public static String toSimpleName( String qualified_name ) {
        int index = qualified_name.lastIndexOf( '.' );
        if (index < 0) {
  	  return qualified_name;
        } else {
  	  return qualified_name.substring( index + 1 );
        }
    }

    /**
     * Converts a fully-qualified name to the corresponding package name.
     *
     * <pre>
     * For example :
     *   toPackageName( "java.lang.Class" ) returns  "java.lang".
     *   toPackageName( "MyClass" ) returns  "".
     * </pre>
     *
     * @return	the given name <I>as is</I> if it is not a qualified name
     */
    public static String toPackageName( String qualified_name ) {
        int index = qualified_name.lastIndexOf( '.' );
        return (index < 0) ? "" : qualified_name.substring( 0, index );
    }
  
    public String currentClassName() {
        if (parent == null)  return null;
        return parent.currentClassName();
    }

    /******************************/
    public boolean isRegisteredModifier( String str ) {
        if (parent == null)  return false;
        return parent.isRegisteredModifier( str );
    }

    /**
     * If this is a {@link ClassEnvironment} for declarerName, record new inner
     * class innerName; otherwise, pass up the environment hierarchy.
     *
     * @param declarerName fully-qualified name of enclosing class
     * @param innerName    simple name of inner class
     */
    public void recordMemberClass(String declarerName, String innerName)
    {
	if (parent != null) parent.recordMemberClass(declarerName, innerName);
    }
}
