/*
 * VariableInitializer.java 1.0
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
 * The VariableInitializer class presents common interfaces
 * to several initializer format
 *
 * interface VariableInitializer is implemented by
 *  Expression
 *  ArrayInitializer
 *
 * @see openjava.ptree.Expression
 * @see openjava.ptree.ArrayInitializer
 */
public interface VariableInitializer extends ParseTree
{
}
