/*
 * Parameter.java 1.0
 *
 * This interface is made to type ptree-node into
 * the parameter in the body of class.
 *
 * Jun 20, 1997 by mich
 * Sep 29, 1997 by bv
 * Oct 10, 1997 by mich
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Sep 29, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;



/**
 * The Parameter class represents parameter node of parse tree.
 * Modifiers of parameter are supported from JDK 1.1.
 * The code like:
 * <br><blockquote><pre>
 *     void test( final int i ){
 *         ....
 *     }
 * </pre></blockquote><br>
 * is allowed from JDK 1.1.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.ModifierList
 */
public class Parameter extends NonLeaf
{

    /**
     * Allocates a new object.
     *
     * @param  modfiers  modifier list of the new parameter
     * @param  type_specifier  type specifier includes array dimension info
     * @param  declname  the parameter's name, including no array dim.
     */
    public Parameter(
	ModifierList	modiflist,
	TypeName	type_specifier,
        String		declname )
    {
	super();
	if (modiflist == null) {
	    modiflist = new ModifierList();
	}
	set( modiflist, type_specifier, declname );
    }

    /**
     * Allocates a new object.
     * 
     *
     * @param type_specifier type specifier includes array dimension info
     * @param declname the parameter's name, also includes array dim
     *        arg modfier is null means parameter has no modifier
     */
    public Parameter(
	TypeName type_specifier,
	String declname )
    {
	this( new ModifierList(), type_specifier, declname );
    }
  
    public void writeCode() {
	writeDebugL();
	
	ModifierList modifs = getModifiers();
	modifs.writeCode();
	if (! modifs.isEmpty()) {
	    out.print( " " );
	}
	
	TypeName typespec = getTypeSpecifier();
	typespec.writeCode();
	
	out.print( " " );
	
	String declname = getVariable();
	out.print( declname );
	
	writeDebugR();
    }
  
    /**
     * Gets the modifiers of this parameter.
     *
     * @return the modfiers.
     */
    public ModifierList getModifiers() {
	return (ModifierList) elementAt( 0 );
    }
    
    /**
     * Sets the modifiers of this parameter.
     *
     * @param  modifs  the modfiers to set.
     */
    public void setModifiers( ModifierList modifs ) {
	setElementAt( modifs, 0 );
    }
    
    /**
     * Gets the type specifier of this parameter.
     *
     * @return the type specifier.
     */
    public TypeName getTypeSpecifier() {
	return (TypeName) elementAt( 1 );
    }

    /**
     * Sets the type specifier of this parameter.
     *
     * @param  tspec  the type specifier to set.
     */
    public void setTypeSpecifier( TypeName tspec ) {
	setElementAt( tspec, 1 );
    }
    
    /**
     * Gets the variable name of this parameter.
     *
     * @return the variable name.
     */
    public String getVariable() {
	return (String) elementAt( 2 );
    }
    
    /**
     * Sets the variable name of this parameter.
     *
     * @param  varname  the variable name to set.
     */
    public void setVariable( String varname ) {
	setElementAt( varname, 2 );
    }
    
    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}
