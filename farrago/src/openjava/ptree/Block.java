/*
 * Block.java 1.0
 *
 * Jun 11, 1997 by mich
 * Aug 29, 1997 by mich
 *
 * @see openjava.ptree.ParseTree
 * @version last updated: Aug 29, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.OutputStream;


/**
 * The Block class represents a node of parse tree
 * of block statement.
 *
 * <p>Here is an example of a block:
 * <br><blockquote><pre>
 *     {
 *         int i = 0;
 *         i = f( i );
 *     }
 * </pre></blockquote><br>
 *
 * @see openjava.ptree.NonLeaf
 * @see openjava.ptree.Statement
 */
public class Block extends NonLeaf
  implements Statement
{
    /**
     * Allocates a new object.
     *
     * @param  stmts  statement list.
     */
    public Block( StatementList stmts ) {
        super();
        if (stmts == null)  stmts = new StatementList();
        set( stmts );
    }

    /**
     * Allocates a new object with an empty statement list.
     *
     */
    public Block() {
        this( new StatementList() );
    }

    public void writeCode() {
        out.println( "{" );

        StatementList stmtlst = getStatements();
        pushNest();  stmtlst.writeCode();  popNest();

        writeTab();
        out.println( "}" );
    }

    /**
     * Gets the statement list of this block.
     *
     * @return  the statement list.
     */
    public StatementList getStatements() {
        return (StatementList) elementAt( 0 );
    }

    /**
     * Sets the statement list of this block.
     *
     * @param  stmts  the statement list to set.
     */
    public void setStatements( StatementList stmts ) {
        setElementAt( stmts, 0 );
    }

    public void accept( ParseTreeVisitor v ) throws ParseTreeException {
        v.visit( this );
    }

}


