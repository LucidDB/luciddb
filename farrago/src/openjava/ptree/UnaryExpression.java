/*
 * UnaryExpression.java 1.0
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
 * The <code>UnaryExpression</code> class presents for an expression which
 * consists of unary operator with one Expression.
 * <br>
 * The unary expressions are :
 * <br><blockquote>
 * <code>expr++</code>, <code>expr--</code>,
 * <code>++expr</code>, <code>--expr</code>,
 * <code>^expr</code>, <code>!expr</code>,
 * <code>+expr</code> or <code>-expr</code>
 * </blockquote><br>
 * ,where <code>expr<code> is an expression.
 * <br>
 * If the operator in the expression of the operand has week unity,
 * this automatically produces the code in which the operand
 * is enclosed by parenthesises.
 * <br>
 * In the case the operand is <code>y = x</code> and
 * the urary operator is <code>+</code>,
 * this produces the code :
 * <br><blockquote><pre>
 *     +(y = x)
 * </pre></blockquote><br>
 *
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Expression
 */
public class UnaryExpression extends NonLeaf
    implements Expression
{
    /**
     * Post increment operator. For example:
     * <br><blockquote><pre>
     *     i++
     * </pre></blockquote><br>
     */
    public static final int POST_INCREMENT = 0;

    /**
     * Post decrement operator. For example:
     * <br><blockquote><pre>
     *     i--
     * </pre></blockquote><br>
     */
    public static final int POST_DECREMENT = 1;

    /**
     * Pre increment operator. For example:
     * <br><blockquote><pre>
     *     ++i
     * </pre></blockquote><br>
     */
    public static final int PRE_INCREMENT = 2;

    /**
     * Post increment operator. For example:
     * <br><blockquote><pre>
     *     --i
     * </pre></blockquote><br>
     */
    public static final int PRE_DECREMENT = 3;

    /**
     * Post increment operator. For example:
     * <br><blockquote><pre>
     *     ~i
     * </pre></blockquote><br>
     */
    public static final int BIT_NOT = 4;

    /**
     * Post increment operator. For example:
     * <br><blockquote><pre>
     *     ! c
     * </pre></blockquote><br>
     */
    public static final int NOT = 5;

    /**
     * Post increment operator. For example:
     * <br><blockquote><pre>
     *     +i
     * </pre></blockquote><br>
     */
    public static final int PLUS = 6;

    /**
     * Post increment operator. For example:
     * <br><blockquote><pre>
     *     -i
     * </pre></blockquote><br>
     */
    public static final int MINUS = 7;

    /**
     * Exists operator. For example:
     * <br><blockquote><pre>
     *     exists (select from emp)
     * </pre></blockquote><br>
     */
    public static final int EXISTS = 8;

    private static final String opr_string[] = {
        "++", "--", "++", "--", "~", "!", "+", "-", "exists"
    };

    /** operator */
    private int opr = -1;

    /**
     * Allocates a new object.
     *
     * @param  opr  the operator of this unary expression.
     * @param  expr  the expression.
     */
    public UnaryExpression( int opr, Expression expr ) {
        super();
        set( (ParseTree) expr );
        this.opr = opr;
    }

    /**
     * Allocates a new object.
     *
     * @param  expr  the expression.
     * @param  opr  the operator of this unary expression.
     */
    public UnaryExpression( Expression expr, int opr ) {
        super();
        set( (ParseTree) expr );
        this.opr = opr;
    }

    UnaryExpression() {
      super();
    }

    public ParseTree makeRecursiveCopy() {
      UnaryExpression result
          = (UnaryExpression) super.makeRecursiveCopy();
      result.opr = this.opr;
      return result;
    }

    public ParseTree makeCopy() {
      UnaryExpression result
          = (UnaryExpression) super.makeCopy();
      result.opr = this.opr;
      return result;
    }

    public void writeCode()
    {
      writeDebugL();

      if(isPrefix()){
        out.print( operatorString() );
      }

      Expression expr = getExpression();
      if(expr instanceof AssignmentExpression
         || expr instanceof ConditionalExpression
         || expr instanceof BinaryExpression
         || expr instanceof InstanceofExpression
         || expr instanceof CastExpression
         || expr instanceof UnaryExpression){
        out.print( "(" );
        expr.writeCode();
        out.print( ")" );
      }else{
        expr.writeCode();
      }

      if(isPostfix()){
        out.print( operatorString() );
      }

      writeDebugR();
    }

    /**
     * Gets the expression operated in this expression.
     *
     * @return  the expression.
     */
    public Expression getExpression() {
      return (Expression) elementAt( 0 );
    }

    /**
     * Sets the expression operated in this expression.
     *
     * @param  expr  the expression to set.
     */
    public void setExpression( Expression expr ) {
      setElementAt( expr, 0 );
    }

    /**
     * Gets the operator of this unary expression.
     *
     * @return  the operator.
     * @see openjava.ptree.UnaryExpression#POST_INCREMENT
     * @see openjava.ptree.UnaryExpression#POST_DECREMENT
     * @see openjava.ptree.UnaryExpression#PRE_INCREMENT
     * @see openjava.ptree.UnaryExpression#PRE_DECREMENT
     * @see openjava.ptree.UnaryExpression#BIT_NOT
     * @see openjava.ptree.UnaryExpression#NOT
     * @see openjava.ptree.UnaryExpression#PLUS
     * @see openjava.ptree.UnaryExpression#MINUS
     * @see openjava.ptree.UnaryExpression#EXISTS
     */
    public int getOperator() {
      return opr;
    }

    /**
     * Sets the operator of this unary expression.
     *
     * @param  opr  the operator id to set.
     * @see openjava.ptree.UnaryExpression#POST_INCREMENT
     * @see openjava.ptree.UnaryExpression#POST_DECREMENT
     * @see openjava.ptree.UnaryExpression#PRE_INCREMENT
     * @see openjava.ptree.UnaryExpression#PRE_DECREMENT
     * @see openjava.ptree.UnaryExpression#BIT_NOT
     * @see openjava.ptree.UnaryExpression#NOT
     * @see openjava.ptree.UnaryExpression#PLUS
     * @see openjava.ptree.UnaryExpression#MINUS
     * @see openjava.ptree.UnaryExpression#EXISTS
     */
    public void setOperator( int opr ) {
      this.opr = opr;
    }

    /**
     * Tests if the operator of unary expression is a postfix operator.
     *
     * @return  true  if the operator is postfix.
     */
    public boolean isPostfix() {
      if(opr == POST_DECREMENT || opr == POST_INCREMENT)
        return true;
      return false;
    }

    /**
     * Tests if the operator of unary expression is a prefix operator.
     *
     * @return  true  if the operator is prefix.
     */
    public boolean isPrefix()
    {
      return !isPostfix();
    }

    public String operatorString() {
        return opr_string[opr];
    }

    public void accept(ParseTreeVisitor v) throws ParseTreeException {
        v.visit(this);
    }

    public OJClass getType( Environment env )
        throws Exception
    {
        return getExpression().getType( env );
    }

}
