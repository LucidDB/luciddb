/*
 * AllocationExpression.java 1.0
 *
 *
 * Jun 20, 1997 by mich
 * Sep 29, 1997 by bv
 * Oct 10, 1997 by mich
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Oct 10, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;



/**
 * The <code>AllocationExpression</code> class represents
 * an expression which allocates a new object with its constructor.
 * <br>
 * This expression is like:
 * <br><blockquote><pre>
 *     new String( "test" )
 * </pre></blockquote><br>
 * or:
 * <br><blockquote><pre>
 *     new String( "test" ){
 *             public void hoge(){ ... }
 *             ...
 *         }
 * </pre></blockquote><br>
 * The latter is supported from JDK 1.1, is called an anoymous class
 * of the inner class.
 * 
 * @see openjava.ptree.Expression
 * @see openjava.ptree.TypeName
 * @see openjava.ptree.ExpressionList
 * @see openjava.ptree.MemberDeclarationList
 */
public class AllocationExpression extends NonLeaf
    implements Expression
{

    /**
     * Allocates a new object with the class body.
     *
     * @param  ctype  a class name to be constructed
     * @param  args  argument list of constructor
     * @param  mdlst  the class body.
     *                If this is null, no class body will be provided
     *                this construct expression with.
     */
    public AllocationExpression(
	Expression		encloser,
	TypeName		typename,
	ExpressionList		args,
	MemberDeclarationList	mdlst )
    {
	super();
	if (args == null)  args = new ExpressionList();
	/* an explicitly specified null has meaning of no body */
	//if (mdlst == null)  mdlst = new MemberDeclarationList();
	set( typename, args, mdlst, encloser );
    }

    /**
     * Allocates a new object with the class body.
     *
     * @param  ctype  a class name to be constructed
     * @param  args  argument list of constructor
     * @param  mdlst  the class body.
     *                If this is null, no class body will be provided
     *                this construct expression with.
     */
    public AllocationExpression(
	TypeName		typename,
	ExpressionList		args,
	MemberDeclarationList	mdlst )
    {
	this( null, typename, args, mdlst );
    }

    /**
     * Allocates a new object with the class body.
     *
     * @param  ctype  a class name to be constructed
     * @param  args  argument list of constructor
     * @param  mdlst  the class body.
     *                If this is null, no class body will be provided
     *                this construct expression with.
     */
    public AllocationExpression(
	Expression		encloser,
	TypeName		typename,
	ExpressionList		args )
    {
	this( encloser, typename, args, null );
    }
  
    /**
     * Allocates a new object without class body.
     *
     * @param  ctype  a class name to be constructed
     * @param  args  argument list of constructor
     */
    public AllocationExpression( TypeName ctype, ExpressionList args ) {
	this( ctype, args, null );
    }

    public AllocationExpression( OJClass type, ExpressionList args ) {
        this( TypeName.forOJClass( type ), args );
    }

    AllocationExpression() {
	super();
    }

    public void writeCode() {
	writeDebugL();

	Expression encloser = getEncloser();
	if (encloser != null) {
	    encloser.writeCode();
	    out.print( " . " );
	}

	out.print( "new " );
    
	TypeName tn = getClassType();
	tn.writeCode();
    
	out.print( "(" );
	ExpressionList args = getArguments();
	if(! args.isEmpty()){
	    out.print(" ");
	    args.writeCode();
	    out.print(" ");
	} else {
	    args.writeCode();
	}
	out.print( ")" );

	MemberDeclarationList mdlst = getClassBody();
	if(mdlst != null){
	    out.println( "{" );
	    pushNest();  mdlst.writeCode();  popNest();
	    writeTab();  out.print( "}" );
	}

	writeDebugR();
    }
  
    /**
     * Gets the expression of enclosing object.
     *
     * @return  the expression of enclosing object
     */
    public Expression getEncloser() {
	return (Expression) elementAt( 3 );
    }

    /**
     * Sets the expression of enclosing object.
     *
     * @param  encloser  the expression of enclosing object
     */
    public void setEncloser( Expression encloser ) {
	setElementAt( encloser, 3 );
    }

    /**
     * Gets the class type of this constructor.
     *
     * @return  the class type of this constructor.
     */
    public TypeName getClassType() {
	return (TypeName) elementAt( 0 );
    }
  
    /**
     * Sets the class type of this constructor.
     *
     * @param  ctype  the class body to set.
     */
    public void setClassType( TypeName ctype ) {
	setElementAt( ctype, 0 );
    }
    
    /**
     * Gets the arguments of this constructor.
     *
     * @return  the arguments as an expressions list.
     */
    public ExpressionList getArguments() {
	return (ExpressionList) elementAt( 1 );
    }
  
    /**
     * Sets the arguments of this constructor.
     *
     * @return  the expressions list of arguments.
     */
    public void setArguments( ExpressionList args ) {
	if (args == null) {
	    args = new ExpressionList();
	}
	setElementAt( args, 1 );
    }
  
    /**
     * Gets the class body of this constructor.
     *
     * @return  the member declaration list as the class body of
     *          this constructor.
     */
    public MemberDeclarationList getClassBody() {
	return (MemberDeclarationList) elementAt( 2 );
    }
  
    /**
     * Sets the class body of this constructor.
     *
     * @param  mdlist  the member declaration list of the class body.
     *                 If this is null, the class body will disappear.
     */
    public void setClassBody( MemberDeclarationList mdlist ) {
	setElementAt( mdlist, 2 );
    }

    public OJClass getType( Environment env )
	throws Exception
    {
	MemberDeclarationList mdlst = getClassBody();
	if (mdlst == null || mdlst.isEmpty()) {
	    String typename = env.toQualifiedName( getClassType().toString() );
	    OJClass result = env.lookupClass( typename );
	    return result;
	} else {
	    ClassEnvironment classEnv = env.getClassEnvironmentParent();
	    return Toolbox.lookupAnonymousClass(classEnv, this);
	}
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }
}
