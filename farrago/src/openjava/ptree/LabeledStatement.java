/*
 * Parameter.java 1.0
 *
 * This interface is made to type ptree-node into
 * the parameter in the body of class.
 *
 * Jun 20, 1997 mich
 * Sep 29, 1997 bv
 * Oct 11, 1997 mich
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
 * The LabeledStatement class presents labeled statement node
 * of parse tree
 *
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Statement
 */
public class LabeledStatement extends NonLeaf
    implements Statement
{

    /**
     * Allocates a new object.
     *
     */
    public LabeledStatement( String  name, Statement   statement ) {
	super();
	set( name, (ParseTree) statement );
    }

    LabeledStatement() {
	super();
    }
  
    public void writeCode() {
	writeTab();

	String name = getLabel();
	out.print( name );
    
	out.println( " :" );
    
	Statement statement = getStatement();
	statement.writeCode();

    }

    /**
     * Gets the label.
     * 
     * @return  the label.
     */
    public String getLabel() {
	return (String) elementAt( 0 );
    }

    /**
     * Sets the label.
     * 
     * @param  label  the label to set.
     */
    public void setLabel( String label ) {
	setElementAt( label, 0 );
    }

    /**
     * Gets the statement of this labeled statement
     *
     * @return  the statement.
     */
    public Statement getStatement() {
	return (Statement) elementAt( 1 );
    }

    /**
     * Sets the statement of this labeled statement
     *
     * @return  stmt  the statement to set.
     */
    public void setStatement( Statement stmt ) {
	setElementAt( stmt, 1 );
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}
