/*
 * BinaryExpression.java 1.0
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
 * The <code>BinaryExpression</code> class represents
 * an expression which consists of an operators and two operands.
 * <br>
 * This doesn't includes the expression whose operator is
 * the <code>instanceof</code> operator
 * nor the expression whose operator is one of the
 * assignment operators.
 * <br>
 * If the operator in the expression of the left operand or
 * the right operand has week unity,
 * this automatically produces the code in which the left operand
 * is enclosed by parenthesises.
 * <br>
 * In the case the left is <code>a + b</code>,
 * the operator is <code>*</code>
 * the right is <code>c + d</code>,
 * this produces the code :
 * <br><blockquote><pre>
 *     (a + b) * (c + d)
 * </pre></blockquote><br>
 *
 * @see openjava.ptree.Expression
 * @see openjava.ptree.InstanceofExpression
 * @see openjava.ptree.AssignmentExpression
 */
public class BinaryExpression extends NonLeaf
    implements Expression
{
    public static final int TIMES = 0;
    public static final int DIVIDE = 1;
    public static final int MOD = 2;
    public static final int PLUS = 3;
    public static final int MINUS = 4;
    public static final int SHIFT_L = 5;
    public static final int SHIFT_R = 6;
    public static final int SHIFT_RR = 7;
    public static final int LESS = 8;
    public static final int GREATER = 9;
    public static final int LESSEQUAL = 10;
    public static final int GREATEREQUAL = 11;
    public static final int INSTANCEOF = 12;
    public static final int EQUAL = 13;
    public static final int NOTEQUAL = 14;
    public static final int BITAND = 15;
    public static final int XOR = 16;
    public static final int BITOR = 17;
    public static final int LOGICAL_AND = 18;
    public static final int LOGICAL_OR = 19;
    public static final int IN = 20;
    public static final int UNION = 21;
    public static final int EXCEPT = 22;
    public static final int INTERSECT = 23;

    static final String[] opr_string = {
      "*", "/", "%", "+", "-", "<<", ">>", ">>>",
      "<", ">", "<=", ">=", "instanceof",
      "==", "!=", "&", "^", "|", "&&", "||",
      "in", "union", "except", "intersect",
    };

    /** the operator */
    private int opr = -1;

    /**
     * Allocates a new object.
     *
     * @param  lexp  the expression of the left operand.
     * @param  opr  the id number of operator.
     * @param  rexp  the expression of the right operand.
     */
    public BinaryExpression(
	Expression		lexp,
	int			opr,
	Expression		rexp )
    {
	super();
	set( (ParseTree) lexp, (ParseTree) rexp );
	this.opr = opr;
    }

    public BinaryExpression(
	Expression		lexp,
	String			opr,
	Expression		rexp )
    {
	this( lexp, 0, rexp );
	for (int i = 0; i < opr_string.length; ++i) {
	    if (opr_string[i].equals( opr ))  this.opr = i;
	}
    }

    BinaryExpression() {
	super();
    }

    public ParseTree makeRecursiveCopy() {
      BinaryExpression result
          = (BinaryExpression) super.makeRecursiveCopy();
      result.opr = this.opr;
      return result;
    }

    public ParseTree makeCopy() {
      BinaryExpression result
          = (BinaryExpression) super.makeCopy();
      result.opr = this.opr;
      return result;
    }

    public void writeCode() {
	writeDebugL();

	Expression lexpr = getLeft();
	if (needsLeftPar( lexpr )) {
	    out.print( "(" );
	    lexpr.writeCode();
	    out.print( ")" );
	} else {
	    lexpr.writeCode();
	}

	out.print( " " + operatorString() + " " );

	Expression rexpr = getRight();
	if (needsRightPar( rexpr )) {
	    out.print( "(" );
	    rexpr.writeCode();
	    out.print( ")" );
	} else {
	    rexpr.writeCode();
	}

	writeDebugR();
    }

    private final boolean needsLeftPar( Expression leftexpr ) {
      if(leftexpr instanceof AssignmentExpression
         || leftexpr instanceof ConditionalExpression){
        return true;
      }

      int op = strength( getOperator() );

      if(leftexpr instanceof InstanceofExpression){
        if(op > strength( INSTANCEOF ))  return true;
        return false;
      }

      if(! (leftexpr instanceof BinaryExpression))  return false;

      BinaryExpression lbexpr = (BinaryExpression) leftexpr;
      if(op > strength( lbexpr.getOperator() ))  return true;
      return false;
    }

    private final boolean needsRightPar( Expression rightexpr ) {
      if(rightexpr instanceof AssignmentExpression
         || rightexpr instanceof ConditionalExpression){
        return true;
      }

      int op = strength( getOperator() );

      if(rightexpr instanceof InstanceofExpression){
        if(op >= strength( INSTANCEOF ))  return true;
        return false;
      }

      if(! (rightexpr instanceof BinaryExpression))  return false;

      BinaryExpression lbexpr = (BinaryExpression) rightexpr;
      if(op >= strength( lbexpr.getOperator() ))  return true;
      return false;
    }

    /**
     * Returns the strength of the union of the operator.
     *
     * @param  op  the id number of operator.
     * @return  the strength of the union.
     */
    protected static final int strength( int op ) {
      switch( op ){
      case TIMES :
      case DIVIDE :
      case MOD :
        return 40;
      case PLUS :
      case MINUS :
        return 35;
      case SHIFT_L :
      case SHIFT_R :
      case SHIFT_RR :
        return 30;
      case LESS :
      case GREATER :
      case LESSEQUAL :
      case GREATEREQUAL :
      case INSTANCEOF :
        return 25;
      case EQUAL :
      case NOTEQUAL :
      case IN :
        return 20;
      case BITAND :
        return 16;
      case XOR :
        return 14;
      case BITOR :
        return 12;
      case LOGICAL_AND :
        return 10;
      case LOGICAL_OR :
        return 8;
      }
      return 100;
    }

    /**
     * Gets the expression of the left operand.
     *
     * @return  the left expression.
     */
    public Expression getLeft() {
      return (Expression) elementAt( 0 );
    }

    /**
     * Sets the expression of the left operand.
     *
     * @param lexpr  the left expression.
     */
    public void setLeft( Expression lexpr ) {
      setElementAt( lexpr, 0 );
    }

    /**
     * Gets the expression of the right operand.
     *
     * @return  the right expression.
     */
    public Expression getRight() {
      return (Expression) elementAt( 1 );
    }

    /**
     * Sets the expression of the right operand.
     *
     * @param  rexpr  the right expression.
     */
    public void setRight( Expression rexpr ) {
      setElementAt( rexpr, 1 );
    }

    /**
     * Gets the id number of the operator.
     *
     * @return  the id number of the operator.
     * @see openjava.ptree.BinaryExpression#TIMES
     * @see openjava.ptree.BinaryExpression#DIVIDE
     * @see openjava.ptree.BinaryExpression#MOD
     * @see openjava.ptree.BinaryExpression#PLUS
     * @see openjava.ptree.BinaryExpression#MINUS
     * @see openjava.ptree.BinaryExpression#SHIFT_L
     * @see openjava.ptree.BinaryExpression#SHIFT_R
     * @see openjava.ptree.BinaryExpression#SHIFT_RR
     * @see openjava.ptree.BinaryExpression#LESS
     * @see openjava.ptree.BinaryExpression#GREATER
     * @see openjava.ptree.BinaryExpression#LESSEQUAL
     * @see openjava.ptree.BinaryExpression#GREATEREQUAL
     * @see openjava.ptree.BinaryExpression#INSTANCEOF
     * @see openjava.ptree.BinaryExpression#EQUAL
     * @see openjava.ptree.BinaryExpression#NOTEQUAL
     * @see openjava.ptree.BinaryExpression#BITAND
     * @see openjava.ptree.BinaryExpression#XOR
     * @see openjava.ptree.BinaryExpression#BITOR
     * @see openjava.ptree.BinaryExpression#LOGICAL_AND
     * @see openjava.ptree.BinaryExpression#LOGICAL_OR
     * @see openjava.ptree.BinaryExpression#IN
     */
    public int getOperator() {
      return this.opr;
    }

    /**
     * Sets the id number of the operator.
     *
     * @param  opr  the id number of the operator.
     * @see openjava.ptree.BinaryExpression#TIMES
     * @see openjava.ptree.BinaryExpression#DIVIDE
     * @see openjava.ptree.BinaryExpression#MOD
     * @see openjava.ptree.BinaryExpression#PLUS
     * @see openjava.ptree.BinaryExpression#MINUS
     * @see openjava.ptree.BinaryExpression#SHIFT_L
     * @see openjava.ptree.BinaryExpression#SHIFT_R
     * @see openjava.ptree.BinaryExpression#SHIFT_RR
     * @see openjava.ptree.BinaryExpression#LESS
     * @see openjava.ptree.BinaryExpression#GREATER
     * @see openjava.ptree.BinaryExpression#LESSEQUAL
     * @see openjava.ptree.BinaryExpression#GREATEREQUAL
     * @see openjava.ptree.BinaryExpression#INSTANCEOF
     * @see openjava.ptree.BinaryExpression#EQUAL
     * @see openjava.ptree.BinaryExpression#NOTEQUAL
     * @see openjava.ptree.BinaryExpression#BITAND
     * @see openjava.ptree.BinaryExpression#XOR
     * @see openjava.ptree.BinaryExpression#BITOR
     * @see openjava.ptree.BinaryExpression#LOGICAL_AND
     * @see openjava.ptree.BinaryExpression#LOGICAL_OR
     * @see openjava.ptree.BinaryExpression#IN
     */
    public void setOperator( int opr ) {
      this.opr = opr;
    }

    public String operatorString() {
	return opr_string[getOperator()];
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

    public OJClass getType( Environment env )
        throws Exception
    {
	switch (this.opr) {
	case LESS :
	case GREATER :
	case LESSEQUAL :
	case GREATEREQUAL :
	case EQUAL :
	case NOTEQUAL :
	case INSTANCEOF :
	case IN :
	    return  OJSystem.BOOLEAN ;
	default :
	    return chooseType( getLeft().getType( env ),
			       getRight().getType( env ) );
	}
    }

    static OJClass chooseType( OJClass left, OJClass right ) {
	int leftst = strength( left ), rightst = strength( right );
	if (leftst == OTHER && rightst == OTHER)  {
	    if (left.isAssignableFrom( right ))  return left;
	    if (right.isAssignableFrom( left ))  return right;
	    return right;
	}
	return ((leftst > rightst) ? left : right);
    }

    private static final int STRING = 30;
    private static final int OTHER = 4;
    private static int strength( OJClass type ) {
	if (type == OJSystem.STRING)	return STRING;
	if (type == OJSystem.DOUBLE)	return 20;
	if (type == OJSystem.FLOAT)	return 18;
	if (type == OJSystem.LONG)	return 16;
	if (type == OJSystem.INT)	return 14;
	if (type == OJSystem.CHAR)	return 12;
	if (type == OJSystem.BYTE)	return 10;
	if (type == OJSystem.NULLTYPE)	return 0;/*****/
	return OTHER;
    }

}
