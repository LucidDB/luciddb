/*
 * $Id$
 *
 * Jul 28, 1998 by mich
 */
package openjava.mop;


import java.io.StringWriter;
import java.io.PrintWriter;
import java.util.Vector;
import java.util.Hashtable;
import java.util.Enumeration;
import openjava.mop.OJClass;


public final class ClassEnvironment
        extends ClosedEnvironment
{
    private String className = null;
    private Vector memberClasses = new Vector();
    /**
     * Maps {@link openjava.ptree.AllocationExpression}s to the names
     * (<code>"1"</code>, etc.) of the inner classes which have been implicitly
     * created for them.  See {@link Toolbox#lookupAnonymousClass}.
     **/
    Hashtable mapAnonDeclToClass = new Hashtable();

    public ClassEnvironment( Environment e, String name ) {
	super( e );
	className = toSimpleName( name );
    }

    public ClassEnvironment( Environment e ) {
	super( e );
	className = null;
    }

    public ClassEnvironment( Environment e, OJClass clazz ) {
        super( e );
	className = toSimpleName( clazz.getName() );
	OJClass[] memclazz = clazz.getDeclaredClasses();
	for (int i = 0; i < memclazz.length; ++i) {
	    memberClasses.addElement( memclazz[i].getSimpleName() );
	    record(memclazz[i].getName(), memclazz[i]);
	}
    }

    // override Environment
    public ClassEnvironment getClassEnvironmentParent()
    {
	return this;
    }

    public String getClassName() {
        if (className != null)  return className;
	return "<unknown class>";
    }

    public Vector getMemberClasses() {
	return memberClasses;
    }

    public String toString() {
        StringWriter str_writer = new StringWriter();
        PrintWriter out = new PrintWriter( str_writer );

        out.println( "ClassEnvironment" );
        out.println( "class : " + getClassName() );
        out.println( "member classes : " + getMemberClasses() );
	out.print( "parent env : " + parent );

        out.flush();
        return str_writer.toString();
    }

    public void recordClassName( String name ) {
	this.className = toSimpleName( name );
    }

    public void recordMemberClass( String name ) {
	memberClasses.addElement( name );
    }

    public void recordMemberClass(String declarerName, String innerName)
    {
	if (declarerName.equals(currentClassName())) {
	    recordMemberClass(innerName);
	} else if (parent != null) {
	    parent.recordMemberClass(declarerName, innerName);
	}
    }

    public OJClass lookupClass( String name ) {
// 	try {
	    OJClass declarer = parent.lookupClass(currentClassName());
	    OJClass[] declaredClasses = declarer.getDeclaredClasses();
	    for (int i = 0; i < declaredClasses.length; i++) {
		if (declaredClasses[i].getName().equals(name)) {
		    return declaredClasses[i];
		}
	    }
	    Enumeration anonClasses = mapAnonDeclToClass.elements();
	    while (anonClasses.hasMoreElements()) {
		OJClass clazz = (OJClass) anonClasses.nextElement();
		if (clazz.getName().equals(name)) {
		    return clazz;
		}
	    }
//	} catch ( OJClassNotFoundException e ) {
//	    System.err.println( "unexpected exception" +
//				currentClassName() + " : " + e.toString() );
//	}
	return parent.lookupClass( name );
    }

    public VariableInfo lookupBind(String name) {
 	try {
//	    OJClass declarer = OJClass.forName(currentClassName());
	    OJClass declarer = lookupClass(currentClassName());
	    OJField field = pickupField(new OJClass[] { declarer }, name);
	    if (field != null) {
			return new BasicVariableInfo(field.getType());
		}
	} catch ( Exception e ) {
        throw Toolbox.newInternal(
            e,
            "unexpected exception looking up " + name + " in "
            + currentClassName());
	}
	return parent.lookupBind(name);
    }

    private static OJField pickupField(OJClass[] reftypes, String name) {
	for (int i = 0; i < reftypes.length; ++i) {
	    try {
		return reftypes[i].getField(name, reftypes[i]);
	    } catch ( NoSuchMemberException e ) {}
	    OJClass[] inners = reftypes[i].getDeclaredClasses();
	    OJField result = pickupField(inners, name);
	    if (result != null)  return result;
	}
	return null;
    }

    /**
     * Obtains the fully-qualified name of the given class name.
     *
     * @param  name  a simple class name or a fully-qualified class name
     * @return  the fully-qualified name of the class
     */
    public String toQualifiedName( String name ) {
        if (name == null)  return null;
        if (name.endsWith("[]")) {
            String stripped = name.substring( 0, name.length() - 2 );
            return toQualifiedName( stripped ) + "[]";
        }
	if (name.indexOf( "." ) != -1) {
	    /* may be simple name + innerclass */
	    String top = getFirst( name );
	    String qtop = toQualifiedName( top );
	    if (qtop == null || qtop.equals( top ))  return name;
	    return qtop + "." + getRest( name );
	}
	if (name.equals( getClassName() )) {
	    /* inner class */
	    String result = askParent( name );
	    if (result != null)  return result;
	    /* top level class */
	    String pack = getPackage();
	    if (pack == null || pack.equals( "" ))  return name;
	    return pack + "." + name;
	}
	if (memberClasses.indexOf( name ) >= 0) {
	    return currentClassName() + "." + name;
	}
	return parent.toQualifiedName( name );
    }

    private static final String getFirst( String qname ) {
	int dot = qname.indexOf( "." ) ;
	if (dot == -1)  return qname;
	return qname.substring( 0, dot );
    }
    private static final String getRest( String qname ) {
	int dot = qname.indexOf( "." ) ;
	if (dot == -1)  return qname;
	return qname.substring( dot + 1 );
    }
    private String askParent( String sname ) {
	Environment ancestor = parent;
	while (ancestor != null && ! (ancestor instanceof ClassEnvironment)) {
	    ancestor = ancestor.parent;
	}
	if (ancestor == null)  return null;
	return ancestor.toQualifiedName( sname );
    }

    public String currentClassName() {
	return toQualifiedName( getClassName() );
    }
}
