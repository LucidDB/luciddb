/*
 * InstanceofExpression.java 1.0
 *
 *
 * Jun 20, 1997 by mich
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
 * The <code>InstanceofExpression</code> represents a call to the
 * <code>instanceof</code> operator.
 *
 * <p>For example:
 * <br><blockquote><pre>
 *     obj instanceof Object
 * </pre></blockquote><br>
 * If the operator in the expression of the left operand has weak unity,
 * this automatically produces the code in which the left operand
 * is enclosed by parentheses.
 * <br>
 * In the case the left is <code>obj = obj2</code> and
 * the right is <code>String</code>,
 * this produces the code :
 * <br><blockquote><pre>
 *     (obj = obj2) instanceof String
 * </pre></blockquote><br>
 *
 * @see openjava.ptree.Expression
 * @see openjava.ptree.TypeName
 */
public class InstanceofExpression extends NonLeaf
    implements Expression
{

    /**
     * Allocates a new object.
     *
     * @param  lexp  the expression to test.
     * @param  tspec  the typespecifier.
     */
    public InstanceofExpression(
        Expression              lexp,
        TypeName                tspec )
    {
        super();
        set( lexp, tspec );
    }

    InstanceofExpression() {
        super();
    }

    public void writeCode() {
        writeDebugL();

        Expression lexpr = getExpression();
        if (needsLeftPar( lexpr )) {
            out.print( "(" );
            lexpr.writeCode();
            out.print( ")" );
        } else {
            lexpr.writeCode();
        }

        out.print( " instanceof " );

        TypeName rexpr = getTypeSpecifier();
        rexpr.writeCode();

        writeDebugR();
    }

    private final boolean needsLeftPar( Expression lexpr ) {
        if (lexpr instanceof AssignmentExpression
            || lexpr instanceof ConditionalExpression) {
            return true;
        }
        /* this is too strict for + */
        if (lexpr instanceof BinaryExpression) {
            return true;
        }
        return false;
    }

    /**
     * Gets the expression of the left operand to be tested
     * in this expression.
     *
     * @return the left expression.
     */
    public Expression getExpression() {
        return (Expression) elementAt( 0 );
    }

    /**
     * Sets the expression of the left operand to be tested
     * in this expression.
     *
     * @param  lexpr  the left expression to set.
     */
    public void setLeft( Expression lexpr ) {
        setElementAt( lexpr, 0 );
    }

    /**
     * Gets the type specifier of the right operand to be tested
     * in this expression.
     *
     * @return  the type specifier.
     */
    public TypeName getTypeSpecifier() {
        return (TypeName) elementAt( 1 );
    }

    /**
     * Sets the type specifier of the right operand to be tested
     * in this expression.
     *
     * @param  tspec  the type specifier to set.
     */
    public void setTypeSpecifier( TypeName tspec ) {
        setElementAt( tspec, 1 );
    }

    public OJClass getType( Environment env )
        throws Exception
    {
        return OJClass.forClass( boolean . class );
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}
