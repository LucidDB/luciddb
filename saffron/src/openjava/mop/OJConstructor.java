/*
 * OJConstructor.java
 *
 * Jul 28, 1998 by mich
 */
package openjava.mop;


import java.lang.reflect.*;
import java.util.*;
import openjava.ptree.*;


public final class OJConstructor implements OJMember
{
    private OJConstructorImp substance;

    private static Hashtable table = new Hashtable();

    OJConstructor( Constructor m ) {
	this.substance = new OJConstructorByteCode( m );
    }

    public OJConstructor( OJClass declarer, OJModifier modif,
			  OJClass[] parameterTypes, OJClass[] exceptionTypes,
			  ConstructorInvocation ci, StatementList body )
    {
        this( declarer, modif,
	      Toolbox.generateParameters( parameterTypes ),
	      exceptionTypes, ci, body );
    }

    public OJConstructor( OJClass declarer, OJModifier modif,
			  OJClass[] parameterTypes, String[] parameterNames,
			  OJClass[] exceptionTypes,
			  ConstructorInvocation ci, StatementList body )
    {
        this( declarer, modif,
	      Toolbox.generateParameters( parameterTypes, parameterNames ),
	      exceptionTypes, ci, body );
    }

    public OJConstructor( OJClass declarer, OJModifier modif,
			  ParameterList params, OJClass[] exceptionTypes,
			  ConstructorInvocation ci, StatementList body )
    {
        Environment env = declarer.getEnvironment();
        ModifierList modiflist = new ModifierList();
        modiflist.add( modif.toModifier() );
        ConstructorDeclaration d = new ConstructorDeclaration(
	    modiflist,
	    Environment.toSimpleName( declarer.getName() ),
	    params,
	    Toolbox.TNsForOJClasses( exceptionTypes ),
	    ci,
	    body
	    );
        this.substance = new OJConstructorSourceCode( env, declarer, d );
    }

    public OJConstructor( Environment env, OJClass declarer,
			  ConstructorDeclaration d )
    {
	this.substance = new OJConstructorSourceCode( env, declarer, d );
    }

    public static OJConstructor forConstructor( Constructor java_constr ) {
	if (java_constr == null)  return null;
        OJConstructor constr = (OJConstructor) table.get( java_constr );
        if (constr == null) {
            constr = new OJConstructor( java_constr );
            table.put( java_constr, constr );
        }
        return constr;
    }

    public static OJConstructor[]
    arrayForConstructors( Constructor[] jconstrs ) {
        OJConstructor[] result = new OJConstructor[jconstrs.length];
        for (int i = 0; i < result.length; ++i) {
            result[i] = forConstructor( jconstrs[i] );
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
	/***********/
        return substance.getName();
    }
  
    public String getIdentifiableName() {
        return substance.getIdentifiableName();
    }
  
    public OJModifier getModifiers() {
        return substance.getModifiers();
    }
  
    public OJClass[] getParameterTypes() {
	return substance.getParameterTypes();
    }
  
    public OJClass[] getExceptionTypes() {
	return substance.getExceptionTypes();
    }

    public ParseTree getSuffix( String keyword ) {
	return substance.getSuffix( keyword );
    }

    /******************/
    public ExpressionList getParameterVariables()
        throws CannotAlterException
    {
        ConstructorDeclaration d = getSourceCode();
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
        ConstructorDeclaration d = getSourceCode();
        ParameterList params = d.getParameters();
        String[] result = new String[params.size()];
        for (int i = 0; i < result.length; ++i) {
            result[i] = params.get( i ).getVariable().toString();
        }
        return result;
    }

    /**
     * Compares this method against the given object.
     * The algorithm is borrowed by java.lang.reflect.Constructor.equals().
     *
     * @see java.lang.reflect.Constructor#equals
     */
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof OJConstructor) {
	    OJConstructor other = (OJConstructor) obj;
	    return ((getDeclaringClass() == other.getDeclaringClass())
		    && compareParameters( other ));
        }
  
        return false;
    }
  
    private boolean compareParameters( OJConstructor other ) {
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
     * by java.lang.reflect.Constructor.hashCode().
     *
     * @see java.lang.reflect.Constructor#hashCode
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
     * Creates a new instance of the constructor's declaring class
     *
     * @exception CannotExecuteException if the constructor is not
     * compiled yet.
     */
    public Object newInstance(Object[] initargs)
        throws InstantiationException, IllegalAccessException,
               IllegalArgumentException, InvocationTargetException,
               InstantiationException, CannotExecuteException
    {
	return substance.newInstance( initargs );
    }
  
    /* -- methods java.lang.reflect.Constructor does not supply. -- */
  
    public final boolean isExecutable() {
        return substance.isExecutable();
    }
  
    public final boolean isAlterable() {
        return substance.isAlterable();
    }

    public final Constructor getByteCode() throws CannotExecuteException {
        return substance.getByteCode();
    }

    public final ConstructorDeclaration getSourceCode()
	throws CannotAlterException
    {
        return substance.getSourceCode();
    }

    public final StatementList getBody() throws CannotAlterException {
        return substance.getBody();
    }

    public final ConstructorInvocation getTransference()
        throws CannotAlterException
    {
        return substance.getTransference();
    }

    /* -- inner use only -- */

    void setDeclaringClass( OJClass parent ) throws CannotAlterException {
        substance.setDeclaringClass( parent );
    }

    /* -- Translation (not overridable) -- */
  
    final public void setModifiers( int mods ) throws CannotAlterException {
        substance.setModifiers( mods );
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

    public final ConstructorInvocation
    setTransference( ConstructorInvocation invocation )
        throws CannotAlterException
    {
        return substance.setTransference( invocation );
    }
  
    public final StatementList setBody( StatementList stmts )
        throws CannotAlterException
    {
        return substance.setBody( stmts );
    }

}


/**
 * The abstract class <code>OJConstructorImp</code> provides an interface to
 * an implementation of OJConstructor.
 */
abstract class OJConstructorImp
{
    public abstract String toString();
    abstract Environment getEnvironment();
    abstract OJClass getDeclaringClass();
    abstract String getName();
    abstract String getIdentifiableName();
    abstract OJModifier getModifiers();
    abstract OJClass[] getParameterTypes();
    abstract OJClass[] getExceptionTypes();
    abstract ParseTree getSuffix( String keyword );

    abstract Object newInstance( Object[] initargs )
        throws InstantiationException, IllegalAccessException,
               IllegalArgumentException, InvocationTargetException,
               InstantiationException, CannotExecuteException;
  
    /* -- methods java.lang.reflect.Constructor does not supply. -- */
  
    abstract boolean isExecutable();
    abstract boolean isAlterable();
    abstract Constructor getByteCode() throws CannotExecuteException;
    abstract ConstructorDeclaration getSourceCode()
	throws CannotAlterException;
    abstract StatementList getBody() throws CannotAlterException;
    abstract ConstructorInvocation getTransference()
        throws CannotAlterException;

    /* -- inner use only -- */
    abstract void setDeclaringClass( OJClass parent )
	throws CannotAlterException;
  
    /* -- Translation (not overridable) -- */
  
    abstract void setModifiers( int mods ) throws CannotAlterException;
    abstract StatementList setBody( StatementList stmts )
        throws CannotAlterException;
    abstract void setExceptionTypes( OJClass[] types )
        throws CannotAlterException;
    abstract ConstructorInvocation
    setTransference( ConstructorInvocation invocation )
        throws CannotAlterException;
  
}


class OJConstructorByteCode extends OJConstructorImp
{

    private Constructor javaConstructor = null;

    OJConstructorByteCode( Constructor java_constr ) {
	this.javaConstructor = java_constr;
    }

    public String toString() {
	return javaConstructor.toString();
    }

    Environment getEnvironment() {
        Environment result
	    = new ClosedEnvironment( getDeclaringClass().getEnvironment() );
	return result;
    }  
  
    OJClass getDeclaringClass() {
        return OJClass.forClass( javaConstructor.getDeclaringClass() );
    }
  
    String getName() {
        return javaConstructor.getName();
    }
  
    String getIdentifiableName() {
	/***********/
        return getDeclaringClass().getName() + "()";
    }
  
    OJModifier getModifiers() {
        return OJModifier.forModifier( javaConstructor.getModifiers() );
    }
  
    OJClass[] getParameterTypes() {
        return OJClass.arrayForClasses( javaConstructor.getParameterTypes() );
    }
  
    OJClass[] getExceptionTypes() {
        return OJClass.arrayForClasses( javaConstructor.getExceptionTypes() );
    }

    ParseTree getSuffix( String keyword ) {
	return null;
    }
    
    Object newInstance( Object[] initargs )
        throws InstantiationException, IllegalAccessException,
               IllegalArgumentException, InvocationTargetException,
               InstantiationException
    {
	return javaConstructor.newInstance( initargs );
    }
  
    /* -- methods java.lang.reflect.Constructor does not supply. -- */
  
    boolean isExecutable() {
        return true;
    }
  
    boolean isAlterable() {
        return false;
    }

    Constructor getByteCode() throws CannotExecuteException {
	return javaConstructor;
    }

    ConstructorDeclaration getSourceCode() throws CannotAlterException {
        throw new CannotAlterException( "getSourceCode()" );
    }

    StatementList getBody() throws CannotAlterException {
        throw new CannotAlterException( "getBody()" );
    }

    ConstructorInvocation getTransference()
        throws CannotAlterException
    {
        throw new CannotAlterException( "getTransference()" );
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

    ConstructorInvocation setTransference( ConstructorInvocation invocation )
        throws CannotAlterException
    {
        throw new CannotAlterException( "setTransference()" );
    }
  
}


class OJConstructorSourceCode extends OJConstructorImp
{

    private static int idCounter = 0;
    private int id;

    private OJClass			declarer;
    private ConstructorDeclaration	definition;
    private Environment			env;

    OJConstructorSourceCode( Environment env, OJClass declarer,
			     ConstructorDeclaration ptree )
    {
        this.declarer = declarer;
        this.definition = ptree;
        this.env = env;
        this.id = idCounter++;
    }

    public String toString() {
        OJClass declarer = getDeclaringClass();
        String declarername
	    = (declarer == null) ? "*" + id : declarer.getName();

	StringBuffer buf = new StringBuffer();
	String modif = getModifiers().toString();
	if (! modif.equals( "" )) {
	    buf.append( modif );
	    buf.append( " " );
	}
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
  
    OJClass getDeclaringClass() {
        return this.declarer;
    }
  
    String getName() {
	OJClass declarer = getDeclaringClass();
        return (declarer == null) ? null : declarer.getName();
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
        return declarername + "()";
    }
  
    OJModifier getModifiers() {
        return OJModifier.forParseTree( definition.getModifiers() );
    }
  
    OJClass[] getParameterTypes() {
        return arrayForParameters( definition.getParameters() );
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
	OJClass[] result = new OJClass[(params == null) ? 0 : params.size()];
	for (int i = 0; i < result.length; ++i) {
	    String tname = params.get( i ).getTypeSpecifier().toString();
	    result[i] = Toolbox.forNameAnyway( env, tname );
	}
	return result;
    }

    private final OJClass[] arrayForTypeNames( TypeName[] typenames ) {
	OJClass[] result
	    = new OJClass[(typenames == null) ? 0 : typenames.length];
	for (int i = 0; i < result.length; ++i) {
	    String tname = typenames[i].toString();
	    result[i] = Toolbox.forNameAnyway( env, tname );
	}
	return result;
    }

    Object newInstance( Object[] initargs )
        throws InstantiationException, IllegalAccessException,
               IllegalArgumentException, InvocationTargetException,
               InstantiationException, CannotExecuteException
    {
	throw new CannotExecuteException( "newInstance()" );
    }
  
    /* -- methods java.lang.reflect.Constructor does not supply. -- */
  
    boolean isExecutable() {
        return false;
    }
  
    boolean isAlterable() {
        return true;
    }

    Constructor getByteCode() throws CannotExecuteException {
        throw new CannotExecuteException( "getByteCode()" );
    }

    ConstructorDeclaration getSourceCode() throws CannotAlterException {
        return definition;
    }

    StatementList getBody() throws CannotAlterException {
        return definition.getBody();
    }

    ConstructorInvocation getTransference()
        throws CannotAlterException
    {
        return definition.getConstructorInvocation();
    }

    /* -- inner use only -- */

    void setDeclaringClass( OJClass parent ) throws CannotAlterException {
        this.declarer = parent;
    }
  
    /* -- Translation (not overridable) -- */
  
    final void setModifiers( int mods ) throws CannotAlterException {
	throw new CannotAlterException( "setModifiers()" );
    }

    final void setExceptionTypes( OJClass[] types )
        throws CannotAlterException
    {
        definition.setThrows( Toolbox.TNsForOJClasses( types ) );
    }
  
    StatementList setBody( StatementList stmts )
        throws CannotAlterException
    {
	StatementList result = definition.getBody();
        definition.setBody( stmts );
	return result;
    }

    ConstructorInvocation setTransference( ConstructorInvocation invocation )
        throws CannotAlterException
    {
        ConstructorInvocation result = definition.getConstructorInvocation();
	definition.setConstructorInvocation( invocation );
        return result;
    }

}
