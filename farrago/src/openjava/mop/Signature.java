/*
 * Signature.java
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


import java.lang.Object;


/**
 * The class <code>Signature</code> represents a signature of members of
 * class; innerclass, field, method, or constructor.
 * <p>
 * Objects are immutable.
 * </pre>
 * <p>
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public final class Signature
{
    public static final int CLASS = 0;
    public static final int FIELD = 1;
    public static final int METHOD = 2;
    public static final int CONSTRUCTOR = 3;

    private int kind;

    private OJClass    returnType;
    private String     name;
    private OJClass[]  parameters;

    public Signature( String name ) {
      	      kind = FIELD;
	/*returnType = null;*/
	      this.name = name;
	parameters = null;
    }

    public Signature( String name, OJClass[] paramtypes ) {
	if (paramtypes == null) paramtypes = new OJClass[0];
      	      kind = METHOD;
	returnType = null;
	      this.name = name;
	parameters = (OJClass[]) paramtypes.clone();
    }

    public Signature( OJClass[] paramtypes ) {
	if (paramtypes == null) paramtypes = new OJClass[0];
      	      kind = CONSTRUCTOR;
	returnType = null;
	      name = null;
	parameters = (OJClass[]) paramtypes.clone();
    }

    public Signature( OJClass clazz ) {
	      kind = CLASS;
	/*returnType = null;*/
	      name = clazz.getName();
	parameters = null;
    }

    public Signature( OJField field ) {
	      kind = FIELD;
	/*returnType = field.getType();*/
	      name = field.getName();
	parameters = null;
    }

    public Signature( OJMethod method ) {
      	      kind = METHOD;
	/*returnType = method.getReturnType();*/
	      name = method.getName();
	parameters = (OJClass[]) method.getParameterTypes().clone();
    }

    public Signature( OJConstructor constructor ) {
	      kind = CONSTRUCTOR;
	/*returnType = null;*/
	      name = null;
	parameters = (OJClass[]) constructor.getParameterTypes().clone();
    }

    protected OJClass[] parameterTypes() {
	return parameters;
    }

    public int kind() {
        return kind;
    }

    /**
     * Returns the <code>String</code> representation of this signature.
     *
     * @return  the string representation of this signature.
     */
    public String toString() {
	if (strCache == null)  strCache = getStringValue();
	return strCache;
    }

    private String strCache = null;

    private String getStringValue() {
	StringBuffer buf = new StringBuffer();
	switch (kind()) {
	case CLASS :
	    buf.append( "class " );
	    buf.append( getName() );
	    break;
	case FIELD :
	    buf.append( "field " );
	    buf.append( getName() );
	    break;
	case METHOD :
	    buf.append( "method " );
	    buf.append( getName() );
	    buf.append( "(" );
	    if (parameterTypes().length != 0) {
		buf.append( parameterTypes()[0] );
	    }
	    for (int i = 1; i < parameterTypes().length; ++i) {
		buf.append(",");
		buf.append(parameterTypes()[i].toString());
	    }
	    buf.append( ")" );
	    break;
	case CONSTRUCTOR :
	    buf.append( "constructor " );
	    buf.append( "(" );
	    if (parameterTypes().length != 0) {
		buf.append( parameterTypes()[0] );
	    }
	    for (int i = 1; i < parameterTypes().length; ++i) {
		buf.append( "," );
		buf.append(parameterTypes()[i].toString());
	    }
	    buf.append( ")" );
	    break;
	}
	return buf.toString();
    }

    public int hashCode() {
	if (hashCodeCache == -1)  hashCodeCache = toString().hashCode();
	return hashCodeCache;
    }

    private int hashCodeCache = -1;

    public boolean equals( Object obj ) {
        if (obj == null)  return false;
        if (! (obj instanceof Signature))  return false;
	return toString().equals( obj.toString() );
    }
	  

    public OJClass getReturnType() {
	return returnType;
    }

    public String getName() {
	return name;
    }

    public OJClass[] getParameterTypes() {
	if (parameterTypes() == null)  return null;
	return (OJClass[]) parameterTypes().clone();
    }

    public boolean equals( Signature sign ) {
	if (sign == null)  return false;
	if (this.kind() != sign.kind())  return false;
	switch (kind()) {
	case CLASS :
	    return false;
	case FIELD :
	    return false;
	case METHOD :
	    return false;
	case CONSTRUCTOR :
	    return compareParams( sign.parameterTypes() );
	}
	return false;
    }

    public boolean strictlyEquals( Signature sign ) {
	/*********/
	if (this == sign )  return true;
	return false;
    }

    private final boolean
    compareParams( OJClass[] params ) {
	if (params == null)  return false;
	if (parameterTypes().length != params.length)  return false;
	for (int i = 0; i < params.length; ++i) {
	    if (parameterTypes()[i] != params[i])  return false;
	}
	return true;
    }

    public boolean isClass() {
	return (kind() == CLASS);
    }
    public boolean isConstructor() {
	return (kind() == CONSTRUCTOR);
    }
    public boolean isField() {
	return (kind() == FIELD);
    }
    public boolean isMethod() {
	return (kind() == METHOD);
    }
    
    public static OJClass commonBaseType( OJClass a, OJClass b ) {
	if (a.isAssignableFrom( b ))  return a;
	if (b.isAssignableFrom( a ))  return b;
	return commonBaseType( a.getSuperclass(), b.getSuperclass() );
    }

    public static OJClass[] commonBaseTypes( OJClass[] a, OJClass b[] ) {
        OJClass[] result = new OJClass[a.length];
        for (int i = 0; i < a.length; ++i) {
            result[i] = commonBaseType( a[i], b[i] );
        }
        return result;
    }

}
