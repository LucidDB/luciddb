/*
 * ExpansionApplier.java
 *
 * comments here.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1998 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.ptree.util;


import java.util.Vector;
import openjava.mop.*;
import openjava.ptree.*;
import openjava.mop.Environment;
import openjava.tools.DebugOut;


/**
 * The class <code>ExpansionApplier</code> is an evaluator of each
 * objects of <code>ParseTree</code> family.  Each methods in
 * this class is invoked from the class <code>EvaluationShuttle</code>.
 * <p>
 * The method <code>evaluateDown()</code> is invoked before evaluating
 * the children of the parse tree object, and <code>evaluateUp()</code>
 * is invoked after the evaluation.
 * <p>
 * For a class <code>P</code> and a object <code>p</code> statically
 * typed as P, the parts in source code each expantion will be applied
 * are:
 * <ul>
 * <li>Allocation <code>new P()</code>
 * <li>ArrayAllocation <code>new P[expr]</code>
 * <li>MethodCall <code>P.m()</code>, <code>p.m()</code>
 * <li>FieldRead  <code>P.f</code>, <code>p.f</code> as a right side value
 * <li>FieldWrite <code>P.f = expr</code>, <code>p.f = expr</code>
 * <li>ArrayAccess <code>ap[expr]</code> for <code>P[] ap;</code>
 * <li>Expression  <code>p</code>
 * </ul>
 * in feature version:
 * <ul>
 * <li>CastExpression <code>(P) expr</code> including implicit cast
 * <li>CastedExpression <code>(Q) p</code> including implicit cast
 * </ul>
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.util.EvaluationShuttle
 */
public class ExpansionApplier extends VariableBinder
{
    public ExpansionApplier( Environment env ) {
	super( env );
    }

    private OJClass getType( Expression p ) throws ParseTreeException {
	OJClass result = null;
	try {
	    result = p.getType( getEnvironment() );
	} catch ( Exception e ) {
	    e.printStackTrace();
	    throw new ParseTreeException( e );
	}
	DebugOut.println( "type eval - " + p + "\t: " + result );
	if (result == null) {
	    System.err.println("cannot resolve the type of expression");
	    System.err.println(p.getClass() + " : " + p);
	    System.err.println(getEnvironment());
	    /*****DebugOut.println(getEnvironment().toString());*/
	    if (p instanceof ArrayAccess) {
		ArrayAccess aaexpr = (ArrayAccess) p;
		Expression refexpr = aaexpr.getReferenceExpr();
		OJClass refexprtype = null;
		OJClass comptype = null;
		try {
		    refexprtype = refexpr.getType(getEnvironment());
		    comptype = refexprtype.getComponentType();
		} catch (Exception ex) {}
		System.err.println(refexpr + " : " + refexprtype + " : " +
				   comptype);
	    }
	}
	return result;
    }

    private OJClass getSelfType() throws ParseTreeException {
	OJClass result;
	try {
	    Environment env = getEnvironment();
	    String selfname = env.currentClassName();
	    result = env.lookupClass(selfname);
	} catch (Exception ex) {
	    throw new ParseTreeException(ex);
	}
	return result;
    }

    /** Converts a type name to a class. Never returns null. **/
    private OJClass getType( TypeName typename ) throws ParseTreeException {
	OJClass result;
	try {
	    Environment env = getEnvironment();
	    String qname = env.toQualifiedName(typename.toString());
	    result = env.lookupClass(qname);
	} catch (Exception ex) {
	    throw new ParseTreeException(ex);
	}
        if (result == null) {
            throw new ParseTreeException("Unknown type '" + typename + "'");
        }
	DebugOut.println( "type eval - class access : " + result );
	return result;
    }

    private OJClass computeRefType( TypeName typename, Expression expr )
	throws ParseTreeException
    {
	if (typename != null)  return getType( typename );
	if (expr != null)  return getType( expr );
	return getSelfType();
    }

    public void visit( AssignmentExpression p ) throws ParseTreeException {
        Expression left = p.getLeft();
	if (! (left instanceof FieldAccess)) {
	    super.visit( p );
	    return;
	}
	FieldAccess fldac = (FieldAccess) left;
	Expression refexpr = fldac.getReferenceExpr();
	TypeName reftype = fldac.getReferenceType();
	Expression value = p.getRight();
	/* custom version of  visit() skipping the field */
	Expression newp;
	newp = this.evaluateDown( p );
	if (newp != p) {
	    p.replace( newp );
	    newp.accept( this );
	    return;
	}

	if (refexpr != null) {
	    refexpr.accept( this );
	} else if (reftype != null) {
	    reftype.accept( this );
	}
	value.accept( this );

	newp = this.evaluateUp( p );
	if (newp != p)  p.replace( newp );
    }

    /**
     * Includes expandAllocation() and expandExpression().
     */
    public Expression evaluateUp(AllocationExpression p)
	throws ParseTreeException
    {
	OJClass type = getType(p);
	Expression newp;
	newp = type.expandAllocation(getEnvironment(), p);
	if (newp != p)  return newp;
	newp = type.expandExpression(getEnvironment(), p);
	if (newp != p)  return newp;
	return super.evaluateUp(p);
    }

    /**
     * Includes expandArrayAccess() and expandExpression().
     */
    public Expression evaluateUp(ArrayAccess p)
	throws ParseTreeException
    {
	OJClass type = getType(p);
	Expression newp;
	newp = type.expandArrayAccess(getEnvironment(), p);
	if (newp != p)  return newp;
	newp = type.expandExpression(getEnvironment(), p);
	if (newp != p)  return newp;
	return super.evaluateUp( p );
    }

    /**
     * Includes expandArrayAllocation() and expandExpression().
     */
    public Expression evaluateUp(ArrayAllocationExpression p)
	throws ParseTreeException
    {
	OJClass type = getType(p);
	Expression newp;
	newp = type.expandArrayAllocation(getEnvironment(), p);
	if (newp != p)  return newp;
	newp = type.expandExpression(getEnvironment(), p);
	if (newp != p)  return newp;
	return super.evaluateUp(p);
    }

    /**
     * Includes expandFieldWrite(), expandAssignmentExpression()
     * and expandExpression().
     */
    public Expression evaluateUp( AssignmentExpression p )
	throws ParseTreeException
    {
	Expression left = p.getLeft();
	if (left instanceof FieldAccess) {
	    FieldAccess fldac = (FieldAccess) left;
	    OJClass reftype = computeRefType( fldac.getReferenceType(),
					      fldac.getReferenceExpr() );
	    if (reftype != getSelfType()) {
	        Expression newp
		    = reftype.expandFieldWrite( getEnvironment(), p );
		if (! (newp instanceof AssignmentExpression))  return newp;
		p = (AssignmentExpression) newp;
	    }
	}

	OJClass type = getType( p );
	if (type != getSelfType()) {
	    Expression newp
		= type.expandAssignmentExpression( getEnvironment(), p );
	    if (! (newp instanceof AssignmentExpression))  return newp;
	    p = (AssignmentExpression) newp;
	    type = getType( p );
	}
	if (type != getSelfType()) {
	    Expression newp = type.expandExpression( getEnvironment(), p );
	    if (! (newp instanceof AssignmentExpression))  return newp;
	    p = (AssignmentExpression) newp;
	}
	return super.evaluateUp( p );
    }

    /**
     * Includes expandExpression().
     */
    public Expression evaluateUp(BinaryExpression p)
	throws ParseTreeException
    {
	OJClass type = getType(p);
	Expression newp;
	newp = type.expandExpression(getEnvironment(), p);
	if (newp != p)  return newp;
	return super.evaluateUp(p);
    }

    /**
     * Includes expandCastExpression(), expandCastedExpression() and
     * expandExpression().
     */
    public Expression evaluateUp(CastExpression p)
	throws ParseTreeException
    {
	OJClass type = getType(p.getExpression());
	Expression newp;
	newp = type.expandCastedExpression(getEnvironment(), p);
	if (newp != p)  return newp;
	newp = type.expandCastExpression(getEnvironment(), p);
	if (newp != p)  return newp;
	newp = type.expandExpression(getEnvironment(), p);
	if (newp != p)  return newp;
	return super.evaluateUp(p);
    }

    /**
     * Includes expandExpression().
     */
    public Expression evaluateUp(ClassLiteral p)
	throws ParseTreeException
    {
	OJClass type = getType(p);
	Expression newp;
	newp = type.expandExpression(getEnvironment(), p);
	if (newp != p)  return newp;
	return super.evaluateUp(p);
    }

    /**
     * Includes expandExpression().
     */
    public Expression evaluateUp(ConditionalExpression p)
	throws ParseTreeException
    {
	OJClass type = getType(p);
	Expression newp;
	newp = type.expandExpression(getEnvironment(), p);
	if (newp != p)  return newp;
	return super.evaluateUp(p);
    }

    /**
     * Includes expandFieldRead() and expandExpression().
     * Not to be applied for itself.
     */
    public Expression evaluateUp( FieldAccess p )
	throws ParseTreeException
    {
	{
	    OJClass reftype = computeRefType( p.getReferenceType(),
					      p.getReferenceExpr() );
	    if (reftype != getSelfType()) {
	        Expression newp
		    = reftype.expandFieldRead( getEnvironment(), p );
		if (newp != p)  return newp;
	    }
	}
	{
	    OJClass type = getType( p );
	    Expression newp = type.expandExpression( getEnvironment(), p );
	    if (! (newp instanceof FieldAccess))  return newp;
	    p = (FieldAccess) newp;
	}
	return super.evaluateUp( p );
    }

    /**
     * Includes expandExpression().
     */
    public Expression evaluateUp(InstanceofExpression p)
	throws ParseTreeException
    {
	OJClass type = getType(p);
	Expression newp;
	newp = type.expandExpression(getEnvironment(), p);
	if (newp != p)  return newp;
	return super.evaluateUp(p);
    }

    /**
     * Includes expandExpression().
     */
    public Expression evaluateUp(Literal p)
	throws ParseTreeException
    {
	OJClass type = getType(p);
	Expression newp;
	newp = type.expandExpression(getEnvironment(), p);
	if (newp != p)  return newp;
	return super.evaluateUp(p);
    }

    /**
     * Includes expandMethodCall() and expandExpression().
     */
    public Expression evaluateUp(MethodCall p)
	throws ParseTreeException
    {
	{
	    OJClass reftype = computeRefType( p.getReferenceType(),
					      p.getReferenceExpr() );
	    if (reftype != getSelfType()) {
	        Expression newp
		    = reftype.expandMethodCall( getEnvironment(), p );
		if (newp != p)  return newp;
	    }
	}
	{
	    OJClass type = getType( p );
	    Expression newp = type.expandExpression( getEnvironment(), p );
	    if (! (newp instanceof MethodCall))  return newp;
	    p = (MethodCall) newp;
	}
	return super.evaluateUp( p );
    }

    /**
     * Includes expandExpression().
     */
    public Expression evaluateUp(SelfAccess p)
	throws ParseTreeException
    {
	OJClass type = getType(p);
	Expression newp;
	newp = type.expandExpression(getEnvironment(), p);
	if (newp != p)  return newp;
	return super.evaluateUp(p);
    }

    /**
     * Includes expandTypeName().
     */
    public TypeName evaluateUp(TypeName p)
	throws ParseTreeException
    {
	OJClass type = getType(p);
	TypeName newp;
	newp = type.expandTypeName(getEnvironment(), p);
	if (newp != p)  return newp;
	return super.evaluateUp(p);
    }

    /**
     * Includes expandExpression().
     */
    public Expression evaluateUp(UnaryExpression p)
	throws ParseTreeException
    {
	OJClass type = getType(p);
	Expression newp;
	newp = type.expandExpression(getEnvironment(), p);
	if (newp != p)  return newp;
	return super.evaluateUp(p);
    }

    /**
     * Includes expandExpression().
     */
    public Expression evaluateUp( Variable p )
	throws ParseTreeException
    {
	OJClass type = getType(p);

	/* special ignorance for variable ? */
	if (type == null)  return p;

	Expression newp;
	newp = type.expandExpression(getEnvironment(), p);
	if (newp != p)  return newp;
	return super.evaluateUp(p);
    }

    /**
     * Includes expandVariableDeclaration().
     */
    public Statement evaluateUp(VariableDeclaration p)
	throws ParseTreeException
    {
	OJClass type = getType(p.getTypeSpecifier());
	Statement newp;
	newp = type.expandVariableDeclaration(getEnvironment(), p);
	if (newp != p)  return newp;
	return super.evaluateUp(p);
    }

}
