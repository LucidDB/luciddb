/*
 * ArrayAllocationExpression.java 1.0
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


import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.ptree.util.ParseTreeVisitor;



/**
 * The <code>ArrayAllocationExpression</code> class represents
 * an expression which allocates a new array object.
 * <br>
 * This expression is like:
 * <br><blockquote><pre>
 *     new Object[2][3]
 * </pre></blockquote><br>
 * or:
 * <br><blockquote><pre>
 *     new String[]{ "this", "is", "a", "test" }
 * </pre></blockquote><br>
 * The latter is supported from JDK 1.1.
 *
 * @see openjava.ptree.Expression
 * @see openjava.ptree.TypeName
 * @see openjava.ptree.ExpressionList
 * @see openjava.ptree.ArrayInitializer
 */
public class ArrayAllocationExpression extends NonLeaf
    implements Expression
{

    /**
     * Allocates a new ptree object.
     *
     * @param  typename  the type name.
     * @param  dimlist  the dimension expression list.
     */
    public ArrayAllocationExpression(
	TypeName typename,
	ExpressionList dimlist )
    {
	this( typename, dimlist, null );
    }

    /**
     * Allocates a new ptree object.
     *
     * @param  typename  the type name.
     * @param  dimlist  the dimension expression list.
     * @param  ainit  the array initializer.
     *                If this is null, no initializer will be
     *                provided this allocation with.
     */
    public ArrayAllocationExpression(
	TypeName typename,
	ExpressionList dimlist,
	ArrayInitializer ainit )
    {
	super();
	if (dimlist == null)  dimlist = new ExpressionList();
	set( typename, dimlist, ainit );
    }

    public ArrayAllocationExpression( OJClass type, ExpressionList args ) {
        this( TypeName.forOJClass( type ), args );
    }

    public ArrayAllocationExpression( OJClass type, ExpressionList args,
				      ArrayInitializer ainit ) {
        this( TypeName.forOJClass( type ), args, ainit );
    }

    ArrayAllocationExpression() {
	super();
    }

    public void writeCode() {
	out.print( "new " );
    
	TypeName tn = getTypeName();
	if(tn != null)
	    tn.writeCode();
    
	ExpressionList dl = getDimExprList();
	if (dl != null) {
	    for (int i = 0; i < dl.size(); ++i) {
		Expression expr = dl.get( i );
		out.print( "[" );
		if (expr != null) {
		    expr.writeCode();
		}
		out.print( "]" );
	    }
	}

	ArrayInitializer ainit = getInitializer();
	if (ainit != null) {
	    ainit.writeCode();
	}
    }

    /**
     * Gets the type name of the array.
     *
     * @return  the type name of the array.
     */
    public TypeName getTypeName() {
	return (TypeName) elementAt( 0 );
    }
  
    /**
     * Sets the type name of the array.
     *
     * @param  typename  the type name of the array.
     */
    public void setTypeName( TypeName typename ) {
	setElementAt( typename, 0 );
    }
    
    /**
     * Gets the dimexpr list of the array.
     *
     * @return  the dimexpr list of the array.
     */
    public ExpressionList getDimExprList() {
	return (ExpressionList) elementAt( 1 );
    }
  
    /**
     * Sets the dimexpr list of the array.
     *
     * @param  dimlist  the dimexpr list of the array.
     */
    public void setDimExprList( ExpressionList dimlist ) {
	setElementAt( dimlist, 1 );
    }
  
    /**
     * Gets the initializer of this array allocation.
     *
     * @return  the initializer.
     */
    public ArrayInitializer getInitializer() {
	return (ArrayInitializer) elementAt( 2 );
    }
  
    /**
     * Sets the initializer of this array allocation.
     *
     * @param  ainit  the initializer.
     *                If this is null, no initializer will be set.
     */
    public void setInitializer( ArrayInitializer ainit ) {
	setElementAt( ainit, 2 );
    }
  
    public OJClass getType( Environment env )
        throws Exception
    {
	TypeName tname = getTypeName();
	if (false) {
	    // commented out by jhyde -- ClassEnvironment is not good at
	    // looking up array classes
	    String dims = TypeName.stringFromDimension( getDimExprList().size() );
	    return env.lookupClass( env.toQualifiedName( tname + dims ) );
	} else {
	    int dimCount = getDimExprList().size();
	    return env.lookupClass(
				env.toQualifiedName( tname.toString() ), dimCount );
	}
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}
