/*
 * Metaclass.java
 *
 * It is used for some conveniences in writing metaclasses.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1999 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.mop;


import openjava.ptree.*;
import openjava.ptree.util.*;
import openjava.syntax.*;


/**
 * The class <code>Metaclass</code> is a metametaclass for metaclasses.
 * You can write a metaclass easily using this class.
 * <p>
 * For example,
 * <pre>
 * public class MyClass instantiates Metaclass extends OJClass
 * {
 *     convenient void f() {
 *         .. String.class ..;
 *         .. oldjavaclass.String.class ..;
 *     }
 *     void g() {
 *         .. String.class ..;
 *     }
 * }
 * </pre>
 *
 * <p>Here, you don't have to write constructors if your program don't
 * concern.  And class literal doesn't represent <b>java.lang.Class</b>
 * object but <b>openjava.mop.OJClass</b>.
 *
 * <p>The above code is to be translated into:
 * <pre>
 * public class MyClass instantiates Metaclass extends OJClass
 * {
 *     void f() {
 *         .. openjava.mop.OJClass.forClass( String.class ) ..;
 *         .. java.lang.String.class ..;
 *     }
 *     void g() {
 *         .. java.lang.String.class ..;
 *     }
 *     void translateDefinition() {
 *         OJClass c1 = openjava.mop.OJClass.forClass( String.class );
 *         OJClass c2 = String.class;
 *     }
 *     public MyClass(Environment e, OJClass d, ClassDeclaration p) {
 *         super(e,d,p);
 *     }
 *     public MyClass(Class c, MetaInfo m) {
 *         super(c,m);
 *     }
 * }
 * </pre>
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class Metaclass extends OJClass
{
    private static final String CONVENIENT = "convenient";

    public void translateDefinition() throws MOPException {
	/* automatic constructors */
        OJConstructor src_constr = makeSrcConstr();
	try {
	    getConstructor( src_constr.getParameterTypes() );
	} catch ( NoSuchMemberException e ) {
	    addConstructor( src_constr );
	}
        OJConstructor byte_constr = makeByteConstr();
	try {
	    getConstructor( byte_constr.getParameterTypes() );
	} catch ( NoSuchMemberException e ) {
	    addConstructor( byte_constr );
	}

	/* class literal */
	OJMethod[] methods = getDeclaredMethods();
	for (int i = 0; i < methods.length; ++i) {
	    if (! methods[i].getModifiers().has( CONVENIENT ))  continue;
	    try {
		ClassLiteralReplacer rep
		    = new ClassLiteralReplacer( methods[i].getEnvironment() );
		methods[i].getBody().accept( rep );
	    } catch ( ParseTreeException e ) {
		System.err.println( e );
	    }
	}
    }

    public static final boolean isRegisteredModifier( String keyword ) {
	if (keyword.equals( CONVENIENT ))  return true;
	return OJClass.isRegisteredModifier( keyword );
    }

    private OJConstructor makeSrcConstr() throws MOPException {
	OJModifier modif = OJModifier.forModifier( OJModifier.PUBLIC );
	OJClass[] paramtypes
	    = new OJClass[] {
		OJClass.forClass( Environment . class ),
		OJClass.forClass( OJClass . class ),
		OJClass.forClass( ClassDeclaration . class ) };
	OJConstructor result
	    = new OJConstructor( this, modif, paramtypes, null,
				 null, new StatementList() );
	ConstructorInvocation ci
	    = new ConstructorInvocation( result.getParameterVariables(),
					 null );
	result.setTransference( ci );
	return result;
    }

    private OJConstructor makeByteConstr() throws MOPException {
	OJModifier modif = OJModifier.forModifier( OJModifier.PUBLIC );
	OJClass[] paramtypes
	    = new OJClass[] {
		OJClass.forClass( Class . class ),
		OJClass.forClass( MetaInfo . class ) };
	OJConstructor result
	    = new OJConstructor( this, modif, paramtypes, null,
				 null, new StatementList() );
	ConstructorInvocation ci
	    = new ConstructorInvocation( result.getParameterVariables(),
					 null );
	result.setTransference( ci );
	return result;
    }

    public Metaclass( Environment outer_env, OJClass declarer,
		     ClassDeclaration ptree ) {
	super( outer_env, declarer, ptree );
    }

    public Metaclass( Class javaclass, MetaInfo minfo ) {
	super( javaclass, minfo );
    }

}
