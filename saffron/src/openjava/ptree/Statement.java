/*
 * Statement.java 1.0
 *
 *
 * Jun 20, 1997 by mich
 * Sep 29, 1997 by bv
 * Oct 11, 1997 by mich
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Oct 11, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;



/**
 * The <code>Statement</code> class presents common statement interface
 * of parse tree.
 * <br>
 * this interface is impelemented by 
 * <blockquote>
 *     EmptyStatement
 *     LabeledStatement
 *     ExpressionStatement
 *   (selection statement)
 *     IfStatement
 *     SwitchStatement
 *   (iteration statement)
 *     WhileStatement
 *     DoWhileStatement
 *     ForStatement
 *   (jump statement)
 *     BreakStatement
 *     ContinueStatement
 *     ReturnStatement
 *     ThrowStatement
 *   (guarding statement)
 *     SynchronizedStatement
 *     TryStatement
 * </blockquote>
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.EmptyStatement
 * @see openjava.ptree.LabeledStatement
 * @see openjava.ptree.ExpressionStatement
 * @see openjava.ptree.IfStatement
 * @see openjava.ptree.SwitchStatement
 * @see openjava.ptree.WhileStatement
 * @see openjava.ptree.DoWhileStatement
 * @see openjava.ptree.ForStatement
 * @see openjava.ptree.BreakStatement
 * @see openjava.ptree.ContinueStatement
 * @see openjava.ptree.ReturnStatement
 * @see openjava.ptree.ThrowStatement
 * @see openjava.ptree.SynchronizedStatement
 * @see openjava.ptree.TryStatement
 */
public interface Statement extends ParseTree
{
}
