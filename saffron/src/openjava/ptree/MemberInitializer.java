/*
 * MemberInitializer.java 1.0
 *
 * Jun 20, 1997
 * Sep 29, 1997
 * Oct 10, 1997
 *
 * @see openjava.ptree.ParseTree
 * @version 1.0 last updated:  Oct 10, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;

/**
 * The InstanceInitilizer class represents instance initializer block
 * of parse tree.
 *
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.MemberDeclaration
 * @see openjava.ptree.StatementList
 */
public class MemberInitializer extends NonLeaf
    implements MemberDeclaration
{
    private boolean isStatic = false;
    /**
     * Allocates a new object.
     *
     * @param  block  the statement list of this instance initializer block
     */
    public MemberInitializer( StatementList block ) {
	this( block, false );
    }
  
    public MemberInitializer( StatementList block, boolean is_static ) {
	super();
	this.isStatic = is_static;
	set( block );
    }
  
    public MemberInitializer() {
	super();
    }
    
    public void writeCode() {
        writeTab();  writeDebugL();

	if (isStatic()) {
	    out.print( "static " );
	}
        out.println( "{" );
    
        StatementList block = getBody();
        if(block != null){
          pushNest();  block.writeCode();  popNest();
        }
    
        writeTab();  out.println( "}" );  writeDebugR();
    }

    public boolean isStatic() {
	return isStatic;
    }

    /**
     * Gets the body of this instance initializer.
     *
     * @return  statement list.
     */
    public StatementList getBody() {
	return (StatementList) elementAt( 0 );
    }
  
    /**
     * Sets the body of this instance initializer.
     *
     * @param  stmts  statement list to set.
     */
    public void setBody( StatementList stmts ) {
	setElementAt( stmts, 0 );
    }
  
    public void accept(ParseTreeVisitor v) throws ParseTreeException {
        v.visit(this);
    }

}
