/*
 * EmptyStatement.java 1.0
 *
 *
 * Jun 20, 1997 by mich
 * Sep 29, 1997 by bv
 * Oct 23, 1997 by mich
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Oct 23, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;



/**
 * The EmptyStatement class represents an empty statement node
 * of parse tree.
 *
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Statement
 */
public class EmptyStatement extends NonLeaf
    implements Statement
{
    /**
     * Allocates a new object.
     *
     * @param statement prototype object
     */
    public EmptyStatement() {
	super();
    }
    
    public void writeCode() {
	writeTab();  writeDebugL();
	
	out.print( ";" );
	
	writeDebugR();  out.println();
    }

    public void accept(ParseTreeVisitor v) throws ParseTreeException {
        v.visit(this);
    }

}
