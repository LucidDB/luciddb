/*
 * AssignmentExpression.java 1.0
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
 * The <code>AssignmentExpression</code> class represents
 * an assignment expression with an assignment operator.
 *
 * @see openjava.ptree.Expression
 */
public class AssignmentExpression extends NonLeaf
    implements Expression
{
    public final static int EQUALS = 0;
    public final static int MULT = 1;
    public final static int DIVIDE = 2;
    public final static int MOD = 3;
    public final static int ADD = 4;
    public final static int SUB = 5;
    public final static int SHIFT_L = 6;
    public final static int SHIFT_R = 7;
    public final static int SHIFT_RR = 8;
    public final static int AND = 9;
    public final static int XOR = 10;
    public final static int OR = 11;
    
    final static String opr_string[] = {
      "=", "*=", "/=", "%=", "+=", "-=",
      "<<=", ">>=", ">>>=", "&=", "^=", "|="
    };
    
    private int opr = -1;
  
    /**
     * Allocates a new object.
     *
     * @param  lexp  the left expression.
     * @param  opr  the id number of the operator.
     * @param  rexp  the right expression.
     */
    public AssignmentExpression(
	Expression		lexp,
	int			opr,
	Expression		rexp )
    {
	super();
	set( (ParseTree) lexp, (ParseTree) rexp );
	this.opr = opr;
    }

    public AssignmentExpression(
        Expression              lexp,
        String                  opr,
        Expression              rexp )
    {
        this( lexp, 0, rexp );
        for (int i = 0; i < opr_string.length; ++i) {
            if (opr_string[i].equals( opr ))  this.opr = i;
        }
    }

    public AssignmentExpression() {
	super();
    }

    public ParseTree makeRecursiveCopy() {
	AssignmentExpression result
	    = (AssignmentExpression) super.makeRecursiveCopy();
	result.opr = this.opr;
	return result;
    }
  
    public ParseTree makeCopy() {
	AssignmentExpression result
	    = (AssignmentExpression) super.makeCopy();
	result.opr = this.opr;
	return result;
    }
  
    public void writeCode() {
	writeDebugL();
	
	Expression lexp = getLeft();
	if (lexp instanceof AssignmentExpression) {
	    out.print( "( " );
	    lexp.writeCode();
	    out.print( " )" );
	} else {
	    lexp.writeCode();
	}
      
	out.print( " " + operatorString() + " " );
      
	Expression rexp = getRight();
	rexp.writeCode();
  
	writeDebugR();
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
     * @param  lexpr  the left expression.
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
     */
    public int getOperator() {
	return this.opr;
    }
  
    /**
     * Sets the id number of the operator.
     *
     * @param  opr  the id number of the operator.
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
        return getLeft().getType( env );
    }
}
