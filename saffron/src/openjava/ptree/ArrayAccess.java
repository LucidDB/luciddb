/*
 * ArrayAccess.java 1.0
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
 * The <code>ArrayAccess</code> represents
 * an array access.
 * <p>
 * An array access is like :
 * <br><blockquote><pre>
 *     a.m[i + 1]
 * </pre></blockquote><br>
 * In this array access expression,
 * you can get <code>a.m</code> by <code>getReferenceExpr()</code>
 * and can get <code>i + 1</code> by <code>getIndexExpr()</code> .
 *
 * @see openjava.ptree.Expression
 */
public class ArrayAccess extends NonLeaf
    implements Expression
{
    public ArrayAccess( Expression expr, Expression index_expr ) {
	super();
	set( expr, index_expr );
    }

    ArrayAccess() {
	super();
    }
  
    public void writeCode() {
	writeDebugL();

	Expression expr = getReferenceExpr();
	if (expr instanceof Leaf
	    || expr instanceof ArrayAccess
	    || expr instanceof FieldAccess
	    || expr instanceof MethodCall
	    || expr instanceof Variable) {
	    expr.writeCode();
	} else {
	    out.print( "(" );
	    expr.writeCode();
	    out.print( ")" );
	}
    
	Expression index_expr = getIndexExpr();
	out.print( "[" );
	index_expr.writeCode();
	out.print( "]" );

	writeDebugR();
    }

    /**
     * Gets the expression of array.
     *
     * @return  the experssion accessed as array.
     */
    public Expression getReferenceExpr() {
	return (Expression) elementAt( 0 );
    }
  
    /**
     * Sets the expression accessed as array.
     *
     * @param  expr  the experssion of array.
     */
    public void setReferenceExpr( Expression expr ) {
	setElementAt( expr, 0 );
    }
    
    /**
     * Gets the dimexpr list.
     *
     * @return  the dimexpr list.
     */
    public Expression getIndexExpr() {
	return (Expression) elementAt( 1 );
    }
  
    /**
     * Sets the dimexpr list.
     *
     * @param  dimexprs  the dimexpr list.
     */
    public void setIndexExpr( Expression dimexprs ) {
	setElementAt( dimexprs, 1 );
    }

    public void accept(ParseTreeVisitor v) throws ParseTreeException {
        v.visit(this);
    }

    public OJClass getType( Environment env )
	throws Exception
    {
        OJClass reftype = getReferenceExpr().getType( env );
        return reftype.getComponentType();
    }

}
