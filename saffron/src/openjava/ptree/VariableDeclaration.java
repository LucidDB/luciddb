/*
 * VariableDeclaration.java 1.0
 *
 * This class is made to type ptree-node into the local variable
 * declaration statement in the body of class.
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
 * The VariableDeclaration class presents
 * local variable declaration statement node of parse tree.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Statement
 * @see openjava.ptree.ModifierList
 * @see openjava.ptree.TypeName
 * @see openjava.ptree.VariableDeclarator
 * @see openjava.ptree.VariableInitializer
 */
public class VariableDeclaration extends NonLeaf
    implements Statement
{
    /**
     * Allocates a new object.
     *
     * @param  modifs  the modifier list of this variable declaration.
     * @param  typespec  the type specifier.
     * @param  vdeclr  the variable declarator.
     */
    public VariableDeclaration(
      ModifierList        modifs,
      TypeName       typespec,
      VariableDeclarator  vdeclr )
    {
        super();
        if(modifs == null){
          modifs = new ModifierList();
        }
        set( modifs, typespec, vdeclr );
    }
  
    /**
     * Allocates a new object.
     *
     * @param  typespec  the type specifier.
     * @param  vdeclr  the variable declarator.
     */
    public VariableDeclaration(
      TypeName       typespec,
      VariableDeclarator  vdeclr )
    {
	this( new ModifierList(), typespec, vdeclr );
    }
  
    /**
     * Allocates a new object.
     *
     * @param  modifs  the modifier list of this variable declaration.
     * @param  typespec  the type specifier.
     * @param  vname  the variable name.
     * @param  vinit  the variable initializer.
     */
    public VariableDeclaration(
      ModifierList         modifs,
      TypeName        typespec,
      String           vname,
      VariableInitializer  vinit )
    {
	this( modifs, typespec, new VariableDeclarator( vname, vinit ) );
    }
  
    /**
     * Allocates a new object.
     *
     * @param  modifs  the modifier list of this variable declaration.
     * @param  typespec  the type specifier.
     * @param  vname  the variable name.
     * @param  vinit  the variable initializer.
     */
    public VariableDeclaration(
      TypeName        typespec,
      String           vname,
      VariableInitializer  vinit )
    {
	this( new ModifierList(), typespec, vname, vinit );
    }
  
    VariableDeclaration() {
	super();
    }
    
    public void writeCode() {
        writeTab();  writeDebugL();
    
        ModifierList modiflist = getModifiers();
        modiflist.writeCode();
        if(! modiflist.isEmpty()){
          out.print( " " );
        }
    
        TypeName typespec = getTypeSpecifier();
        typespec.writeCode();
    
        out.print( " " );
        
        VariableDeclarator vd = getVariableDeclarator();
        vd.writeCode();
        
        out.print( ";" );
    
        writeDebugR();  out.println();
    }
    
    /**
     * Gets the modifer list of this variable declaration.
     *
     * @return  the modifier list.
     */
    public ModifierList getModifiers() {
	return (ModifierList) elementAt( 0 );
    }
  
    /**
     * Sets the modifer list of this variable declaration.
     *
     * @param  modifs  the modifier list to set.
     */
    public void setModifiers( ModifierList modifs ) {
	setElementAt( modifs, 0 );
    }
  
    /**
     * Gets the type specifier of this variable declaration.
     * Any modification on obtained objects is never reflected on
     * this object.
     *
     * @return  the type specifier for this variable.
     */
    public TypeName getTypeSpecifier() {
	TypeName result;
	TypeName tn = (TypeName) elementAt(1);
	VariableDeclarator vd = (VariableDeclarator) elementAt(2);
	result = (TypeName) tn.makeCopy();
	result.addDimension(vd.getDimension());
	return result;
    }
  
    /**
     * Sets the type specifier of this variable declaration.
     *
     * @param  tspec  the type specifier to set.
     */
    public void setTypeSpecifier( TypeName tspec ) {
	setElementAt( tspec, 1 );
    }
    
    /**
     * Gets the variable declarator of this variable declaration.
     *
     * @return  the variable declarator.
     */
    public VariableDeclarator getVariableDeclarator() {
	return (VariableDeclarator) elementAt( 2 );
    }
  
    /**
     * Sets the variable declarator of this variable declaration.
     *
     * @param  vdeclr  the variable declarator to set.
     */
    public void setVariableDeclarator( VariableDeclarator vdeclr ) {
	setElementAt( vdeclr, 2 );
    }
    
    /**
     * Gets declarator name, declarator name includes variable name
     * but its dimension.
     * 
     * @return declarator name
     */
    public String getVariable() {
	return getVariableDeclarator().getVariable();
    }
  
    /**
     * Sets declarator name, declarator name includes variable name
     * but its dimension.
     * 
     * @param  name  declarator name to set.
     * @see  openjava.ptree.TypeName
     */
    public void setVariable( String name ) {
	getVariableDeclarator().setVariable( name );
    }
    
    /**
     * Gets variable initializer.
     *
     * @return variable initializer.
     */
    public VariableInitializer getInitializer() {
	return getVariableDeclarator().getInitializer();
    }
  
    /**
     * Sets variable initializer.
     *
     * @param  vinit  the variable initializer to set.
     */
    public void setInitializer( VariableInitializer vinit ) {
	getVariableDeclarator().setInitializer( vinit );
    }
  
    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}
