/*
 * ConditionalExpression.java 1.0
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



/**
 * The <code>ConditionalExpression</code> class represents
 * a conditional expression.
 *
 * <p>Here is an example:
 * <br><blockquote><pre>
 *     (i == 1) ? 3 : 4
 * </pre></blockquote><br>
 * This consists of a conditional part, true case part, and
 * false case part.
 * Each part of them is an expression.
 * <br>
 * If the operator in the expression of the operands has weak unity,
 * this automatically produces the code in which the operands
 * are enclosed by parenthesises.
 * <br>
 * In the case the conditional part is <code>f = f()</code>,
 * the true case part is <code>"red"</code>
 * and the false case part is <code>str = "blue"</code>
 * this produces the code :
 * <br><blockquote><pre>
 *     (f = f()) ? "red" : (str = "blue")
 * </pre></blockquote><br>
 *
 * @see openjava.ptree.Expression
 */
public class ConditionalExpression extends NonLeaf
    implements Expression
{
    /**
     * Allocates a new conditional expression object.
     *
     * @param  condition  the conditional part of this expression.
     * @param  truecase  the expression to be evaluated when conditional
     *                   part is true.
     * @param  falsecase  the expression to be evaluated when conditional
     *                    part is false.
     */
    public ConditionalExpression(
        Expression  condition,
        Expression  truecase,
        Expression  falsecase )
    {
        super();
        set( condition, truecase, falsecase );
    }

    ConditionalExpression() {
        super();
    }

    public void writeCode() {
        writeDebugL();

        Expression condition = getCondition();
        if(condition instanceof AssignmentExpression
           || condition instanceof ConditionalExpression){
            out.print( "(" );
            condition.writeCode();
            out.print( ")" );
        } else {
            condition.writeCode();
        }

        out.print( " ? " );

        Expression truecase = getTrueCase();
        if (truecase instanceof AssignmentExpression) {
            out.print( "(" );
            truecase.writeCode();
            out.print( ")" );
        } else {
            truecase.writeCode();
        }

        out.print( " : " );

        Expression falsecase = getFalseCase();
        if (falsecase instanceof AssignmentExpression) {
            out.print( "(" );
            falsecase.writeCode();
            out.print( ")" );
        } else {
            falsecase.writeCode();
        }

        writeDebugR();
    }

    /**
     * Gets the conditional part of this conditional expression.
     *
     * @return  the expression of this conditional part.
     */
    public Expression getCondition() {
        return (Expression) elementAt( 0 );
    }

    /**
     * Sets the conditional part of this conditional expression.
     *
     * @param  expr  the expression to set as this conditional part.
     */
    public void setCondition( Expression expr ) {
        setElementAt( expr, 0 );
    }

    /**
     * Gets the true case part of this conditional expression.
     *
     * @return  the expression of this true case part.
     */
    public Expression getTrueCase() {
        return (Expression) elementAt( 1 );
    }

    /**
     * Sets the true case part of this conditional expression.
     *
     * @param  expr  the expression to set as this true part.
     */
    public void setTrueCase( Expression expr ) {
        setElementAt( expr, 1 );
    }

    /**
     * Gets the false case part of this.
     *
     * @return  the expression of this false case part.
     */
    public Expression getFalseCase() {
        return (Expression) elementAt( 2 );
    }

    /**
     * Sets the false case part of this.
     *
     * @param  expr  the expression to set as this false part.
     */
    public void setFalseCase( Expression expr ) {
        setElementAt( expr, 2 );
    }

    public void accept(ParseTreeVisitor v) throws ParseTreeException {
        v.visit(this);
    }

    public OJClass getType( Environment env )
        throws Exception
    {
        return BinaryExpression.chooseType( getTrueCase().getType( env ),
                                            getFalseCase().getType( env ) );
    }

}
