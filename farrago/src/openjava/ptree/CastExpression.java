/*
 * CastExpression.java 1.0
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
 * The <code>CastExpression</code> class represents
 * a cast expression of parse tree.
 * <br>
 * If the operator in the expression of the right operand has week unity,
 * this automatically produces the code in which the right operand
 * is enclosed by parenthesises.
 * <br>
 * In the case the caster is <code>int</code> and
 * the right operand to be casted is <code>p + q</code>,
 * this produces the code :
 * <br><blockquote><pre>
 *     (int) (p + q)
 * </pre></blockquote><br>
 *
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Expression
 * @see openjava.ptree.TypeName
 */
public class CastExpression extends NonLeaf
    implements Expression
{
    /**
     * Allocates a new object.
     *
     * @param  ts  the type specifier to cast in this expression.
     * @param  expr  the expression to be casted in this expression.
     */
    public CastExpression( TypeName ts, Expression expr ) {
        super();
        set( (ParseTree) ts, (ParseTree) expr );
    }

    public CastExpression( OJClass type, Expression expr ) {
        this( TypeName.forOJClass( type ), expr );
    }
  
    CastExpression() {
        super();
    }

    /**
     * @deprecated
     */
    public void writeCode() {
        writeDebugL();
    
        out.print( "(" );
        TypeName ts = getTypeSpecifier();
        ts.writeCode();
        out.print( ") " );
    
        Expression expr = getExpression();
        if (expr instanceof AssignmentExpression
	    || expr instanceof ConditionalExpression
	    || expr instanceof BinaryExpression
	    || expr instanceof InstanceofExpression
	    || expr instanceof UnaryExpression){
	    out.print( "( " );
	    expr.writeCode();
	    out.print( " )" );
        } else {
          expr.writeCode();
        }
    
        writeDebugR();
    }
  
    /**
     * Gets the type specifier to cast in this expression.
     *
     * @return  the type specifier.
     */
    public TypeName getTypeSpecifier() {
	return (TypeName) elementAt( 0 );
    }
  
    /**
     * Sets the type specifier to cast in this expression.
     *
     * @param  tspec  the type specifier.
     */
    public void setTypeSpecifier( TypeName tspec ) {
	setElementAt( tspec, 0 );
    }
      
    /**
     * Gets the expression of the operand to be casted in this expression.
     *
     * @return  the expression.
     */
    public Expression getExpression() {
	return (Expression) elementAt( 1 );
    }
  
    /**
     * Sets the expression of the operand to be casted in this expression.
     *
     * @param  expr  the expression.
     */
    public void setExpression( Expression expr ) {
	setElementAt( expr, 1 );
    }

    public void accept(ParseTreeVisitor v) throws ParseTreeException {
        v.visit(this);
    }

    public OJClass getType( Environment env )
	throws Exception
    {
		TypeName typeName = getTypeSpecifier();
		String qname = env.toQualifiedName( typeName.getName() );
        return env.lookupClass( qname, typeName.getDimension() );
    }
  
}
