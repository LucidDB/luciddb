/*
 * Expression.java 1.0
 *
 *
 * Jun 20, 1997
 * Sep 29, 1997
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Sep 29, 1997
 * @author  Teruo -bv- Koyanagi
 */
package openjava.ptree;

import openjava.mop.Environment;
import openjava.mop.OJClass;



/**
 * The Expression interface presents common interface
 * to access Expression node of parse tree.
 *
 * <p>This interface is implemented by:
 * <ul>
 *   <li>{@link UnaryExpression}</li>
 *   <li>{@link BinaryExpression}</li>
 *   <li>{@link ConditionalExpression}</li>
 *   <li>{@link AssignmentExpression}</li>
 *   <li>{@link CastExpression}</li>
 *   <li>{@link AllocationExpression}</li>
 *   <li>{@link ArrayAllocationExpression}</li>
 *   <li>{@link Variable}</li>
 *   <li>{@link MethodCall}</li>
 *   <li>{@link Literal}</li>
 *   <li>{@link ClassLiteral}</li>
 *   <li>{@link ArrayAccess}</li>
 *   <li>{@link FieldAccess}</li>
 * </ul>
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.VariableInitializer
 */
public interface Expression extends ParseTree, VariableInitializer
{
    public OJClass getType( Environment env )
        throws Exception;

    public OJClass getRowType( Environment env )
        throws Exception;
}
