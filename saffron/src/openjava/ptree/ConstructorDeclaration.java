/*
 * ConstructorDeclaration.java 1.0
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


import java.util.Hashtable;
import openjava.ptree.util.*;
import openjava.mop.*;
import openjava.ptree.util.PartialParser;


/**
 * The <code>ConstructorDeclaration</code> class represents
 * constructor declaration node of the parse tree.
 *
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.MemberDeclaration
 * @see openjava.ptree.ModifierList
 * @see openjava.ptree.ParameterList
 * @see openjava.ptree.TypeName
 * @see openjava.ptree.ConstructorInvocation
 * @see openjava.ptree.StatementList
 */
public class ConstructorDeclaration extends NonLeaf
    implements MemberDeclaration
{

    private Hashtable suffixes = null;

    /**
     * Constructs new ConstructorDeclaration from its elements.
     * 
     * @param  modiflist  modifier list, if it has no modifier list
     *                    then thes arg is set empty list.
     * @param  name  name of this constructor.
     * @param  params  parameter list
     * @param  throwlist  throw type list, if there is no throws
     *                    then this arg is set empty list
     * @param  scstmt  statement which calls another constructor
     *                 if this is null, it means no another constructor
     *                 call exists.
     * @param  stmtlist  statement list of this constructor body.
     *                   if this is null, it means method body is with
     *                   only semi colon.
     */
    public ConstructorDeclaration(
	ModifierList		modiflist,
	String			name,
	ParameterList		params,
	TypeName[]		throwlist,
	ConstructorInvocation	scstmt,
	StatementList		stmtlist )
    {
	super();
	if (modiflist == null)  modiflist = new ModifierList();
	if (params == null)     params = new ParameterList();
	if (throwlist == null)  throwlist = new TypeName[0];
	set( modiflist, name, params, throwlist, scstmt, stmtlist );
    }

    /**
     * Constructs new ConstructorDeclaration from its elements.
     * 
     * @param  modiflist  modifier list, if it has no modifier list
     *                    then thes arg is set empty list.
     * @param  name  name of this constructor.
     * @param  params  parameter list
     * @param  throwlist  throw type list, if there is no throws
     *                    then this arg is set empty list
     * @param  stmtlist  statement list of this constructor body.
     *                   if this is null, it means method body is with
     *                   only semi colon.
     */
    public ConstructorDeclaration(
	ModifierList		modiflist,
	String			name,
	ParameterList		params,
	TypeName[]		throwlist,
	StatementList		stmtlist )
    {
	this( modiflist, name, params, throwlist, null, stmtlist );
    }

    /** for recursive copy */
    ConstructorDeclaration() {
	super();
    }

    public void writeCode() {
	writeTab();  writeDebugL();

	ModifierList ml = getModifiers();
	ml.writeCode();
	if(! ml.isEmpty()){
	    out.print( " " );
	}

	String name = getName();
	out.print( name );

	out.print( "(" );

	ParameterList params = getParameters();
	if(params.size() != 0){
	    out.print( " " );
	    params.writeCode();
	    out.print( " " );
	}

	out.print( ")" );
    
	TypeName[] tnl = getThrows();
	if (tnl != null && tnl.length != 0) {
	    out.println();
	    out.print( "throws" );
	    for (int i = 0; i < tnl.length; ++i) {
		out.print( " " );
		tnl[i].writeCode();
	    }
	}

	StatementList body = getBody();
	ConstructorInvocation sc = getConstructorInvocation();
	if (body == null && sc == null) {
	    out.println( ";" );
	} else {
	    out.println();

	    writeTab();  out.println( "{" );
	    pushNest();
	    
	    if (sc != null)  sc.writeCode();
	    if (body != null)  body.writeCode();
	    
	    popNest();
	    
	    writeTab();  out.print( "}" );
	}

	writeDebugR();  out.println();
    }
  
    /**
     * Gets modifier list.
     *
     * @return modifier list.
     */
    public ModifierList getModifiers() {
	return (ModifierList) elementAt( 0 );
    }
  
    /**
     * Sets modifier list.
     *
     * @param  modifs  modifier list.
     */
    public void setModifiers( ModifierList modifs ) {
	if (modifs == null) {
	    modifs = new ModifierList();
	}
	setElementAt( modifs, 0 );
    }
    
    /**
     * Gets the name of this constructor node.
     *
     * @return constructor declarator node.
     */
    public String getName() {
	return (String) elementAt( 1 );
    }
  
    /**
     * Sets the name of this constructor node.
     *
     * @param  name  the name to be set.
     */
    public void setName( String name ) {
	setElementAt( name, 1 );
    }
  
    /**
     * Gets the parameter list.
     *
     * @return parameter list for constructor.
     */
    public ParameterList getParameters() {
	return (ParameterList) elementAt( 2 );
    }
  
    /**
     * Sets the parameter list.
     *
     * @param  params  parameterlist for constructor declarator node.
     */
    public void setParameters( ParameterList params ) {
	if (params == null) {
	    params = new ParameterList();
	}
	setElementAt( params, 2 );
    }
    
    /**
     * Gets the class type list thrown by this constructor.
     *
     * @return class type list thrown by this constructor.
     */
    public TypeName[] getThrows() {
	return (TypeName[]) elementAt( 3 );
    }
  
    /**
     * Sets the class type list thrown by this constructor.
     *
     * @param  ctlist  class type list thrown by this constructor.
     */
    public void setThrows( TypeName[] ctlist ) {
        if (ctlist == null) {
	    ctlist = new TypeName[0];
        }
        setElementAt( ctlist, 3 );
    }
    
    /**
     * Gets the special call statement.
     * Special call statement is like:
     * <br><blockquote><pre>
     *     super();
     * </pre></blockquote><br>
     *
     * @return  special call statement
     */
    public ConstructorInvocation getConstructorInvocation() {    
	return (ConstructorInvocation) elementAt( 4 );
    }
  
    /**
     * Sets the special call statement.
     *
     * @param  scstmt  the special call statement to set
     * @see openjava.ptree.ConstructorDeclaration#getConstructorInvocation()
     */
    public void setConstructorInvocation( ConstructorInvocation scstmt) {    
	setElementAt( scstmt, 4 );
    }
  
    /**
     * Gets the statement list of this constructor body.
     *
     * @return  the statement list of this constructor body.
     */
    public StatementList getBody() {    
	return (StatementList) elementAt( 5 );
    }
  
    /**
     * Sets the statement list of this constructor body.
     *
     * @return  the statement list of this constructor body.
     */
    public void setBody( StatementList stmts ) {    
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
