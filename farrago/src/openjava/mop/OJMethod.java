/*
 * OJMethod.java
 *
 * Jul 28, 1998 by mich
 */
package openjava.mop;


import java.lang.reflect.*;
import java.util.*;
import openjava.ptree.MethodDeclaration;
import openjava.ptree.*;


public final class OJMethod implements OJMember
{
    private OJMethodImp substance;

    private static Hashtable table = new Hashtable();

    OJMethod( Method m ) {
	this.substance = new OJMethodByteCode( m );
    }

    /**
     * Constructs a new <code>OJMethod</code> object.
     * <p>
     * This constructor automatically generates parameter variables.
     */
    public OJMethod( OJClass declarer, OJModifier modif, OJClass returnType,
		     String name, OJClass[] parameterTypes,
		     OJClass[] exceptionTypes, StatementList body )
    {
        this( declarer, modif, returnType, name,
	      Toolbox.generateParameters( parameterTypes ),
	      exceptionTypes, body );
    }

    /**
     * Constructs a new <code>OJMethod</code> object.
     */
    public OJMethod( OJClass declarer, OJModifier modif, OJClass returnType,
		     String name, OJClass[] parameterTypes,
		     String[] parameterNames,
		     OJClass[] exceptionTypes, StatementList body )
    {
        this( declarer, modif, returnType, name,
	      Toolbox.generateParameters( parameterTypes, parameterNames ),
	      exceptionTypes, body );
    }

    /**
     * Constructs a new <code>OJMethod</code> object.
     */
    public OJMethod( OJClass declarer, OJModifier modif, OJClass returnType,
		     String name, ParameterList params,
		     OJClass[] exceptionTypes, StatementList body )
    {
        Environment env = declarer.getEnvironment();
        ModifierList modiflist = new ModifierList();
	modiflist.add( modif.toModifier() );
        MethodDeclaration d = new MethodDeclaration(
	    modiflist,
	    TypeName.forOJClass( returnType ),
	    name,
	    params,
	    Toolbox.TNsForOJClasses( exceptionTypes ),
	    body
	    );
	this.substance = new OJMethodSourceCode( env, declarer, d );
    }

    public OJMethod( Environment env, OJClass declarer, MethodDeclaration d ) {
	this.substance = new OJMethodSourceCode( env, declarer, d );
    }

    /**
     * Generates a method object which has the same attributes as the
     * model method except its body.
     * <p>
     * The body of generated method is to be set to null.
     *
     * @param original  the base model for generating method object.
     */
    public static OJMethod makePrototype( OJMethod original ) {
	return new OJMethod( original.getDeclaringClass(),
			     original.getModifiers(),
			     original.getReturnType(),
			     original.getName(),
			     original.getParameterTypes(),
			     original.getExceptionTypes(),
			     null
			     );
    }

    public static OJMethod forMethod( Method java_method ) {
	if (java_method == null)  return null;
        OJMethod method = (OJMethod) table.get( java_method );
        if (method == null) {
            method = new OJMethod( java_method );
            table.put( java_method, method );
        }
        return method;
    }

    public static OJMethod[] arrayForMethods( Method[] jmethods ) {
        OJMethod[] result = new OJMethod[jmethods.length];
        for (int i = 0; i < result.length; ++i) {
            result[i] = forMethod( jmethods[i] );
        }
        return result;
    }

    public Signature signature() {
        return new Signature( this );
    }
  
    public OJClass getDeclaringClass() {
        return substance.getDeclaringClass();
    }
  
    public String getName() {
        return substance.getName();
    }
  
    public String getIdentifiableName() {
        return substance.getIdentifiableName();
    }
  
    public OJModifier getModifiers() {
        return substance.getModifiers();
    }
  
    public OJClass getReturnType() {
	return substance.getReturnType();
    }
  
    public OJClass[] getParameterTypes() {
	return substance.getParameterTypes();
    }
  
    public OJClass[] getExceptionTypes() {
	return substance.getExceptionTypes();
    }

    /******************/
    public ExpressionList getParameterVariables()
	throws CannotAlterException
    {
        MethodDeclaration d = getSourceCode();
	ParameterList params = d.getParameters();
	ExpressionList result = new ExpressionList();
	for (int i = 0, len = params.size(); i < len; ++i) {
	    result.add( new Variable( params.get( i ).getVariable() ) );
	}
	return result;
    }

    public String[] getParameters()
	throws CannotAlterException
    {
        MethodDeclaration d = getSourceCode();
	ParameterList params = d.getParameters();
	String[] result = new String[params.size()];
	for (int i = 0; i < result.length; ++i) {
	    result[i] = params.get( i ).getVariable().toString();
	}
	return result;
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
    
    /**
     * Compares this method against the given object.
     * The algorithm is borrowed by java.lang.reflect.Method.equals().
     *
     * @see java.lang.reflect.Method#equals
     */
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof OJMethod) {
	    OJMethod other = (OJMethod) obj;
	    return (getDeclaringClass() == other.getDeclaringClass())
		&& (getName().equals( other.getName() ))
		    && compareParameters( other );
        }
  
        return false;
    }
  
    private boolean compareParameters( OJMethod other ) {
        return compareParameters( other.getParameterTypes() );
    }
  
    private boolean compareParameters( OJClass[] params2 ) {
        OJClass[] params1 = getParameterTypes();
        if (params1.length != params2.length)  return false;
	for (int i = 0; i < params1.length; ++i) {
	    if(params1[i] != params2[i])  return false;
	}
	return true;
    }
  
    /**
     * Computes a hashcode for this method.  The algorithm is borrowed
     * by java.lang.reflect.Method.hashCode().
     *
     * @see java.lang.reflect.Method#hashCode
     */
    public int hashCode() {
        return toString().hashCode();
    }
  
    public String toString() {
	return substance.toString();
    }

    public Environment getEnvironment() {
        return substance.getEnvironment();
    }
  
    /**
     * Invokes this method on the given object with the given parameters.
     *  
     * @exception CannotExecuteException if this method is not compiled yet.
     */
    public Object invoke( Object obj, Object[] args )
  	throws IllegalAccessException, IllegalArgumentException,
  	       InvocationTargetException, CannotExecuteException
    {
	return substance.invoke( obj, args );
    }
  
    /* -- methods java.lang.reflect.Method does not supply. -- */
  
    public final boolean isExecutable() {
        return substance.isExecutable();
    }

    public final boolean isAlterable() {
        return substance.isAlterable();
    }

    public final Method getByteCode() throws CannotExecuteException {
        return substance.getByteCode();
    }

    public final MethodDeclaration getSourceCode()
	throws CannotAlterException
    {
        return substance.getSourceCode();
    }

    public final StatementList getBody() throws CannotAlterException {
        return substance.getBody();
    }

    /* -- inner use only -- */

    void setDeclaringClass( OJClass parent ) throws CannotAlterException {
        substance.setDeclaringClass( parent );
    }
  
    /* -- Translation (not overridable) -- */
  
    public final void setName( String name ) throws CannotAlterException {
        substance.setName( name );
    }
  
    public final void setModifiers( int mods ) throws CannotAlterException {
        substance.setModifiers( mods );
    }

    public final void setModifiers( OJModifier mods )
	throws CannotAlterException
    {
	setModifiers( mods.toModifier() );
    }
  
    public final void setReturnType( OJClass type )
	throws CannotAlterException
    {
        substance.setReturnType( type );
    }

    public final void setExceptionTypes( OJClass[] types )
	throws CannotAlterException
    {
        substance.setExceptionTypes( types );
    }

    public final void addExceptionType( OJClass type )
	throws CannotAlterException
    {
	OJClass[] etypes = getExceptionTypes();
	OJClass[] result = new OJClass[etypes.length + 1];
	System.arraycopy( etypes, 0, result, 0, etypes.length );
	result[etypes.length] = type;
        setExceptionTypes( result );
    }

    public final StatementList setBody( StatementList stmts )
	throws CannotAlterException
    {
	return substance.setBody( stmts );
    }

}


/**
 * The abstract class <code>OJMethodImp</code> provides an interface to
 * an implementation of OJMethod.
 */
abstract class OJMethodImp
{
    public abstract String toString();
    abstract OJClass getDeclaringClass();
    abstract String getName();
    abstract String getIdentifiableName();
    abstract OJModifier getModifiers();
    abstract OJClass getReturnType();
    abstract OJClass[] getParameterTypes();
    abstract OJClass[] getExceptionTypes();
    abstract ParseTree getSuffix( String keyword );

    abstract Environment getEnvironment();

    abstract Object invoke( Object obj, Object[] args )
  	throws IllegalAccessException, IllegalArgumentException,
  	       InvocationTargetException, CannotExecuteException;
  
    /* -- methods java.lang.reflect.Method does not supply. -- */
  
    abstract boolean isAlterable();
    abstract boolean isExecutable();
    abstract Method getByteCode() throws CannotExecuteException;
    abstract MethodDeclaration getSourceCode() throws CannotAlterException;
    abstract StatementList getBody() throws CannotAlterException;

    /* -- inner use only -- */

    abstract void setDeclaringClass( OJClass parent )
	throws CannotAlterException;
  
    /* -- Translation (not overridable) -- */
  
    abstract void setName( String name ) throws CannotAlterException;
    abstract void setModifiers( int mods ) throws CannotAlterException;
    abstract void setReturnType( OJClass type ) throws CannotAlterException;
    abstract void setExceptionTypes( OJClass[] types )
	throws CannotAlterException;
    abstract StatementList setBody( StatementList stmts )
	throws CannotAlterException;
  
}


class OJMethodByteCode extends OJMethodImp
{

    private Method javaMethod = null;

    OJMethodByteCode( Method java_method ) {
	this.javaMethod = java_method;
    }

    public String toString() {
	return javaMethod.toString();
    }
  
    OJClass getDeclaringClass() {
        return OJClass.forClass( javaMethod.getDeclaringClass() );
    }
  
    String getName() {
        return javaMethod.getName();
    }
  
    String getIdentifiableName() {
	/***********/
        return getDeclaringClass().getName() + "." + getName() + "()";
    }
  
    OJModifier getModifiers() {
        return OJModifier.forModifier( javaMethod.getModifiers() );
    }
  
    OJClass getReturnType() {
	return OJClass.forClass( javaMethod.getReturnType() );
    }
  
    OJClass[] getParameterTypes() {
        return OJClass.arrayForClasses( javaMethod.getParameterTypes() );
    }
  
    OJClass[] getExceptionTypes() {
        return OJClass.arrayForClasses( javaMethod.getExceptionTypes() );
    }
    
    ParseTree getSuffix( String keyword ) {
	return null;
    }
    
    Environment getEnvironment() {
        Environment result
	    = new ClosedEnvironment( getDeclaringClass().getEnvironment() );
	return result;
    }

    Object invoke( Object obj, Object[] args )
  	throws IllegalAccessException, IllegalArgumentException,
  	       InvocationTargetException, CannotExecuteException
    {
	return javaMethod.invoke( obj, args );
    }
  
    /* -- methods java.lang.reflect.Method does not supply. -- */
  
    boolean isAlterable() {
        return false;
    }
  
    boolean isExecutable() {
        return true;
    }

    Method getByteCode() throws CannotExecuteException {
	return javaMethod;
    }

    MethodDeclaration getSourceCode() throws CannotAlterException {
	throw new CannotAlterException( "getSourceCode()" );
    }

    StatementList getBody() throws CannotAlterException {
	throw new CannotAlterException( "getBody()" );
    }

    /* -- inner use only -- */

    void setDeclaringClass( OJClass parent ) throws CannotAlterException {
        throw new CannotAlterException( "setDeclaringClass()" );
    }
  
    /* -- Translation (not overridable) -- */
  
    final void setName( String name ) throws CannotAlterException {
	throw new CannotAlterException( "setName()" );
    }
  
    final void setModifiers( int mods ) throws CannotAlterException {
	throw new CannotAlterException( "setModifiers()" );
    }
  
    final void setReturnType( OJClass type ) throws CannotAlterException {
	throw new CannotAlterException( "setReturnType()" );
    }
  
    final void setExceptionTypes( OJClass[] types )
	throws CannotAlterException
    {
	throw new CannotAlterException( "setExceptionTypes()" );
    }

    StatementList setBody( StatementList stmts )
	throws CannotAlterException
    {
	throw new CannotAlterException( "setBody()" );
    }

}


class OJMethodSourceCode extends OJMethodImp
{

    private static int idCounter = 0;
    private int id;

    private OJClass		declarer;
    private MethodDeclaration	definition;
    private Environment		env;

    OJMethodSourceCode( Environment env, OJClass declarer,
                        MethodDeclaration ptree )
    {
        this.declarer = declarer;
        this.definition = ptree;
        this.env = env;
        this.id = idCounter++;
    }

    public String toString() {
        OJClass declarer = getDeclaringClass();
        String declarername = (declarer == null) ? "*" : declarer.getName();

	StringBuffer buf = new StringBuffer();
	String modif = getModifiers().toString();
	if (! modif.equals( "" )) {
	    buf.append( modif );
	    buf.append( " " );
	}
	buf.append( getReturnType().getName() );
	buf.append( " " );
	buf.append( declarername );
	buf.append( "." );
	buf.append( getName() );
	buf.append( "(" );
	OJClass[] paramtypes = getParameterTypes();
	if (paramtypes.length != 0) {
	    buf.append( paramtypes[0].getName() );
	}
	for (int i = 1; i < paramtypes.length; ++i) {
	    buf.append( "," );
	    buf.append( paramtypes[i].getName() );
	}
	buf.append( ")" );
	return buf.toString();
    }
  
    OJClass getDeclaringClass() {
        return this.declarer;
    }
  
    String getName() {
        return definition.getName();
    }
  
    String getIdentifiableName() {
        OJClass declarer = getDeclaringClass();
        String declarername;
        if (declarer == null) {
            declarername = "*" + id;
        } else {
            declarername = declarer.getName();
        }
	/***************/
        return declarername + "." + getName() + "()";
    }
  
    OJModifier getModifiers() {
        return OJModifier.forParseTree( definition.getModifiers() );
    }
  
    OJClass getReturnType() {
	String type_name = definition.getReturnType().toString();
	return Toolbox.forNameAnyway( env, type_name );
    }

    private OJClass[] ptypeCache = null;
    private TypeName[] paramtypes = null;
    private boolean isPtypeCacheDirty(TypeName[] paramtypes) {
	if (ptypeCache == null)  return true;
	if (paramtypes.length != this.paramtypes.length)  return true;
	for (int i = 0; i < paramtypes.length; ++i) {
	    if (! paramtypes[i].equals(this.paramtypes[i]))  return true;
	}
	return false;
    }
    private void refleshPtypeCache() {
	ParameterList plist = definition.getParameters();
	int psize = plist.size();
	TypeName[] paramtypes = new TypeName[psize];
	for (int i = 0; i < psize; ++i) {
	    paramtypes[i] = plist.get(i).getTypeSpecifier();
	}
	if (isPtypeCacheDirty(paramtypes)) {
	    ptypeCache = arrayForParameters(plist);
	    this.paramtypes = paramtypes;
	}
    }

    /*
     * This method was:
     *   return arrayForParameters(definition.getParameters());
     * but is tuned up for time efficiency.
     */
    OJClass[] getParameterTypes() {
	refleshPtypeCache();
	OJClass[] result = new OJClass[ptypeCache.length];
	for (int i = 0; i < result.length; ++i)  result[i] = ptypeCache[i];
        return result;
    }

    String[] getParameters() {
	ParameterList params = definition.getParameters();
	String[] result = new String[params.size()];
	for (int i = 0; i < result.length; ++i) {
	    result[i] = params.get( i ).getVariable().toString();
	}
	return result;
    }
  
    OJClass[] getExceptionTypes() {
        return arrayForTypeNames( definition.getThrows() );
    }

    ParseTree getSuffix( String keyword ) {
        Hashtable table = definition.getSuffixes();
        if (table == null)  return null;
        return (ParseTree) table.get( keyword );
    }

    private final OJClass[] arrayForParameters( ParameterList params ) {
	OJClass[] result = new OJClass[params.size()];
	for (int i = 0; i < result.length; ++i) {
	    String tname = params.get( i ).getTypeSpecifier().toString();
	    result[i] = Toolbox.forNameAnyway( env, tname );
	}
	return result;
    }

    private final OJClass[] arrayForTypeNames( TypeName[] typenames ) {
	OJClass[] result = new OJClass[typenames.length];
	for (int i = 0; i < result.length; ++i) {
	    String tname = typenames[i].toString();
	    result[i] = Toolbox.forNameAnyway( env, tname );
	}
	return result;
    }

    Environment getEnvironment() {
        Environment result
	    = new ClosedEnvironment( getDeclaringClass().getEnvironment() );
	OJClass[] ptypes = getParameterTypes();
	String[] pvars = getParameters();
	for (int i = 0; i < ptypes.length; ++i) {
	    result.bindVariable( pvars[i], ptypes[i] );
	}
	return result;
    }

    /**
     * Invokes this method on the given object with the given parameters.
     *  
     * @exception CannotExecuteException if this method is not compiled yet.
     */
    Object invoke( Object obj, Object[] args )
  	throws IllegalAccessException, IllegalArgumentException,
  	       InvocationTargetException, CannotExecuteException
    {
	throw new CannotExecuteException( "invoke()" );
    }
  
    /* -- methods java.lang.reflect.Method does not supply. -- */
  
    boolean isAlterable() {
        return true;
    }
  
    boolean isExecutable() {
        return false;
    }

    Method getByteCode() throws CannotExecuteException {
	throw new CannotExecuteException( "getByteCode()" );
    }

    MethodDeclaration getSourceCode() throws CannotAlterException {
	return definition;
    }

    StatementList getBody() throws CannotAlterException {
	return definition.getBody();
    }

    /* -- inner use only -- */

    void setDeclaringClass( OJClass parent ) throws CannotAlterException {
        this.declarer = parent;
    }

    /* -- Translation (not overridable) -- */

    final void setName( String name ) throws CannotAlterException {
        definition.setName( name );
    }
  
    final void setModifiers( int mods ) throws CannotAlterException {
	definition.setModifiers( new ModifierList( mods ) );
    }
  
    final void setReturnType( OJClass type ) throws CannotAlterException {
	definition.setReturnType( TypeName.forOJClass( type ) );
    }
  
    final void setExceptionTypes( OJClass[] types )
	throws CannotAlterException
    {
	definition.setThrows( Toolbox.TNsForOJClasses( types ) );
    }

    StatementList setBody( StatementList stmts ) throws CannotAlterException {
	StatementList result = definition.getBody();
	definition.setBody( stmts );
	return result;
    }
  
}
