/*
 * MemberAccessCorrector.java
 *
 * Firstly, the parser generates a temporal parse tree.
 * This class correct them.
 * <p>
 *
 * All the continuous field access are stored in a single Variable ptree
 * object.
 * [p.p.f.f].f  [p.f].m()  ([] a single Variable object)
 * FieldAccess := Variable name
 * MemberAccess := Variable name "(" .. ")"
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1998 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.ptree.util;


import openjava.mop.*;
import openjava.ptree.*;
import openjava.tools.DebugOut;


/**
 * Replaces variable references with member references, if the identifier
 * resolves to a member in the current scope.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class MemberAccessCorrector extends VariableBinder
{

    private String errorState = null;

    public MemberAccessCorrector( Environment env ) {
        super( env );
    }

    public String getErrorState() {
        return errorState;
    }

    public Expression evaluateDown( FieldAccess ptree )
        throws ParseTreeException
    {
        super.evaluateDown( ptree );

        if (ptree.getReferenceType() != null)  return ptree;
        if (ptree instanceof TableReference) {
            // TableReference is already correct by construction
            return ptree;
        }

        Expression ref = ptree.getReferenceExpr();
        String field_name = ptree.getName();

        if (ref == null) {
            if (isVariable( field_name )) {
                /* this is a variable. */
                DebugOut.println( "MC variable - " + field_name );
                return new Variable( field_name );
            } else if (isField( field_name )) {
                /* this is a field access */
                DebugOut.println( "MC field access - " + field_name );
            } else {
                /* unknown variable or field */
                System.err.println( "unknown field or variable : "
                                   + field_name );
                System.err.println( getEnvironment() );
                throw new ParseTreeException(
                    "unknown field or variable: " + field_name);
            }
        } else if (ref instanceof Variable) {
            FieldAccess fa = name2fieldaccess( ref.toString(), field_name );
            TypeName typename = fa.getReferenceType();
            Expression refexpr = fa.getReferenceExpr();
            if (typename != null) {
                ptree.setReferenceType( typename );
            } else {
                ptree.setReferenceExpr( refexpr );
            }
        }

        return ptree;
    }

    public Expression evaluateDown( MethodCall ptree )
        throws ParseTreeException
    {
        super.evaluateDown( ptree );

        if (ptree.getReferenceType() != null)  return ptree;

        Expression ref = ptree.getReferenceExpr();
        if (ref == null || ! (ref instanceof Variable))  return ptree;
        String method_name = ptree.getName();

        if (ref instanceof Variable) {
            FieldAccess fa = name2fieldaccess( ref.toString(), method_name );
            TypeName typename = fa.getReferenceType();
            Expression refexpr = fa.getReferenceExpr();
            if (typename != null) {
                ptree.setReferenceType( typename );
            } else {
                ptree.setReferenceExpr( refexpr );
            }
        }
        return ptree;
    }

    private FieldAccess name2fieldaccess( String names, String field ) {
        Expression result_expr;
        String first = getFirst( names );
        String rest = getRest( names );

        if (isVariable( first )) {
            /* this is a variable  */
            DebugOut.println( "MC variable - " + first );
            result_expr = new Variable( first );
        } else if (isField( first )) {
            /* this is a field  */
            DebugOut.println( "MC field - " + first );
            result_expr = new FieldAccess( (Variable) null, first );
        } else {
            /* this is a class */
            while (rest != null && ! isClass( first )) {
                first = first + "." + getFirst( rest );
                rest = getRest( rest );
            }
            if (rest != null) {
                while (isClass( first + "." + getFirst( rest ) )) {
                    first = first + "." + getFirst( rest );
                    rest = getRest( rest );
                }
            }
            if (isClass( first )) {
                DebugOut.println( "MC class - " + first );
            } else {
                System.err.println( "unknown class : " + first );
            }

            TypeName type = new TypeName( first );
            if (rest == null) {
                /* ref is a typename */
                return new FieldAccess( type, field );
            }
            first = getFirst( rest );
            rest = getRest( rest );
            result_expr = new FieldAccess( type, first );
        }

        /* remainder is field access */
        while (rest != null) {
            first = getFirst( rest );
            rest = getRest( rest );
            result_expr = new FieldAccess( result_expr, first );
        }

        return new FieldAccess( result_expr, field );
    }

    private boolean isVariable( String name ) {
        Environment env = getEnvironment();
        return env.isBind( name );
    }

    private boolean isField( String name ) {
        Environment env = getEnvironment();
        String qcname = env.toQualifiedName( env.currentClassName() );
        OJClass declarer = env.lookupClass( qcname );
        OJField field = null;
        while (declarer != null && field == null) {
            try {
                field = declarer.getField( name, declarer );
            } catch ( NoSuchMemberException e ) {}
            declarer = declarer.getDeclaringClass();
        }
        return (field != null);
    }

    private boolean isClass( String name ) {
        Environment env = getEnvironment();
        String qname = env.toQualifiedName( name );
        try {
            OJClass.forName( qname );
            return true;
        } catch ( OJClassNotFoundException e ) {}
        OJClass clazz = env.lookupClass( qname );
        return (clazz != null);
    }

    private static final String getFirst( String qname ) {
        if (qname == null)  return null;
        int dot = qname.indexOf( '.' );
        if (dot == -1)  return qname;
        return qname.substring( 0, dot );
    }

    private static final String getRest( String qname ) {
        if (qname == null)  return null;
        int dot = qname.indexOf( '.' );
        if (dot == -1)  return null;
        return qname.substring( dot + 1 );
    }

}

