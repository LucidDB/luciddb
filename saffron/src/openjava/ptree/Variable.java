/*
 * $Id$
 *
 * This interface is made to type ptree-node into the type
 * specifier in the body of class.
 *
 * Jun 20, 1997 by mich
 * Sep 29, 1997 by bv
 * Oct 11, 1997 by mich
 * Dec 27, 1998 by mich	
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Oct 11, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.mop.Toolbox;
import openjava.ptree.util.ParseTreeVisitor;

import org.eigenbase.util.Util;

/**
 * The <code>Variable</code> class represents a type specifier
 * node of parse tree.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 */
public class Variable extends Leaf implements Expression
{
    private static int variableID = 0;

    /**
     * Allocates a new object.
     *
     * @param  name  name of variable
     */
    public Variable( String name ) {
	super( name );
    }

    public OJClass getType( Environment env )
        throws Exception
    {
	String name = toString();
	int dot = name.indexOf(".");
	if (dot >= 0) {
	    // This variable is compound -- e.g. "x.y.z" -- and
	    // MemberAccessCorrector has not done its job yet.  This happens,
	    // for example, in "Emp[] emps; for (i in (select emp.name from
	    // emps as emp)) { ... }", where we need to determine the type of
	    // "i" before we recurse into the "select".  Temporarily split it
	    // into a FieldAccess and a Variable.
	    Variable variable = new Variable(name.substring(0, dot));
	    FieldAccess fieldAccess = new FieldAccess(
		variable, name.substring(dot + 1));
	    return fieldAccess.getType(env);
	}
		final Environment.VariableInfo info = env.lookupBind( toString() );
		if (info == null) {
            throw Util.newInternal("cannot find variable '" + name + "' in scope");
        }
        return info.getType();
    }

    /**
     * Generates an uniquely named variable
     */
    public static Variable generateUniqueVariable() {
        String name = "oj_var" + variableID;
	++variableID;
        return new Variable( name );
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }
  
}
