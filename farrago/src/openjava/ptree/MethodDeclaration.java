/*
 * MethodDeclaration.java 1.0
 *
 * Jun 20, 1997 by mich
 * Sep 29, 1997 by bv
 * Oct 10, 1997 by mich
 * Jul 29, 1998 by mich, Fixed makeCopy() which crushed.
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Oct 10, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import java.util.Hashtable;
import openjava.ptree.util.*;
import openjava.mop.*;


/**
 * The MethodDeclaration class presents method declaration node
 * of parse tree.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.FieldDeclaration
 */
public class MethodDeclaration extends NonLeaf
    implements MemberDeclaration
{

    private Hashtable suffixes = null;

    /**
     * Constructs new MethodDeclaration from its elements.
     * 
     * @param  modiflist  modifier list. If it has no modifier list
     *                    then thes arg is set empty list.
     * @param  typespec  returning type specifier 
     * @param  methoddecl  method declarator
     * @param  throwlist  throw type list. If there is no throws
     *                    then this arg is set empty list
     * @param  block  method block. if arg block is null, it means method
     *                body with only semi colon such as methods in interface
     */
    public MethodDeclaration(
	ModifierList		modiflist,
	TypeName		typespec,
	String			name,
	ParameterList		params,
	TypeName[]		throwlist,
	StatementList		block )
    {
	super();
	if (params == null)  params = new ParameterList();
	if (throwlist == null)  throwlist = new TypeName[0];
	set( modiflist, typespec, name, params, throwlist, block );
    }

    /**
     * Is needed for recursive copy.
     */
    MethodDeclaration() {
	super();
    }
  
    public void writeCode() {
      writeTab();  writeDebugL();
  
      ModifierList ml = getModifiers();
      ml.writeCode();
      if(! ml.isEmpty()){
        out.print( " " );
      }
      
      TypeName ts = getReturnType();
      ts.writeCode();
  
      out.print( " " );
  
      String name = getName();
      out.print( name );
  
      ParameterList params = getParameters();
      out.print( "(" );
      if(! params.isEmpty()){
        out.print( " " );  params.writeCode();  out.print( " " );
      }else{
        params.writeCode();
      }
      out.print( ")" );
      
      TypeName[] tnl = getThrows();
      if (tnl != null && tnl.length > 0) {
  	out.println();  writeTab();
  	out.print( "throws " );
  	tnl[0].writeCode();
  	for (int i = 1; i < tnl.length; ++i) {
  	    out.print ( ", " );
  	    tnl[i].writeCode();
  	}
      }
  
      StatementList bl = getBody();
      if(bl == null){
        out.print( ";" );
      }else{
        out.println();  writeTab();
        out.print( "{" );
        if(bl.isEmpty()){
  	bl.writeCode();
        }else{
  	out.println();
  	pushNest();  bl.writeCode();  popNest();
  	writeTab();
        }      
        out.print( "}" );
      }
  
      writeDebugR();  out.println();
    }
    
    /**
     * Gets modifierlist of this method.
     *
     * @return  modifier list. Even if there is no modifiers, getModifiers
     *          returns empty list
     */
    public ModifierList getModifiers()
    {
      return (ModifierList) elementAt( 0 );
    }
  
    /**
     * Sets modifierlist of this method.
     *
     * @param  modifs  modifier list to set
     */
    public void setModifiers( ModifierList modifs )
    {
      setElementAt( modifs, 0 );
    }
    
    /**
     * Gets type specifier of this method.
     *
     * @return  type specifier node
     */
    public TypeName getReturnType()
    {
      return (TypeName) elementAt( 1 );
    }
  
    /**
     * Sets type specifier of this method.
     *
     * @param  tspec  type specifier to set
     */
    public void setReturnType( TypeName tspec )
    {
      setElementAt( tspec, 1 );
    }
    
    /**
     * Gets name of this method.
     *
     * @return method declarator node
     */ 
    public String getName()
    {
      return (String) elementAt( 2 );
    }
  
    /**
     * Sets name of this method.
     *
     * @param  name  method's name
     */ 
    public void setName( String name )
    {
      setElementAt( name, 2 );
    }
  
    /**
     * Gets parameter list of this method.
     * Even if this method has no parameter, this returns
     * an empty list of parameter.
     *
     * @return  method's name
     */ 
    public ParameterList getParameters()
    {
      return (ParameterList) elementAt( 3 );
    }
  
    /**
     * Sets parameter list of this method.
     *
     * @param  params  parameter list to set
     */ 
    public void setParameters( ParameterList params )
    {
      setElementAt( params, 3 );
    }
    
    /**
     * Gets throw type name list of this method.
     * Even if there is no throws, this returns an empty list.
     *
     * @return  class type list
     */
    public TypeName[] getThrows()
    {
      return (TypeName[]) elementAt( 4 );
    }
  
    /**
     * Sets throw type name list of this method.
     *
     * @param  class type list to set
     */
    public void setThrows( TypeName[] thrwlist )
    {
      setElementAt( thrwlist, 4 );
    }
    
    /**
     * Gets body of this method.
     * If the body is only semi colon such as the methods' body of interface,
     * this returns null.
     *
     * @return  statement list
     */
    public StatementList getBody()
    {
      return (StatementList) elementAt( 5 );
    }
  
    /**
     * Sets body of this method.
     *
     * @param  stmts  statement list to set
     */
    public void setBody( StatementList stmts )
    {
      setElementAt( stmts, 5 );
    }

    public void setSuffixes( Hashtable suffixes ) {
        this.suffixes = suffixes;
    }

    public Hashtable getSuffixes() {
        return this.suffixes;
    }
  
    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }
}
