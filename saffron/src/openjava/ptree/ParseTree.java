/*
 * ParseTree.java 1.0
 *
 * This subclass of symbol represents (at least) terminal symbols returned 
 * by the scanner and placed on the parse stack.  At present, this 
 * class does nothing more than its super class.
 *
 * Jun 11, 1997
 * Sep 5, 1997 
 *  
 * @see java_cup.runtime.symbol
 * @version 1.0 last updated: Sep 5, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree;


import java.lang.System;
import openjava.ptree.util.*;
import openjava.mop.*;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.StringTokenizer;
import openjava.tools.WriterStack;
import openjava.mop.Environment;


/**
 * The ParseTree class presents for the node of parse tree.
 * This may be a token node, Leaf, or may be a nonterminal node, NonLeaf.
 *
 * @see openjava.ptree.Leaf
 * @see openjava.ptree.NonLeaf
 */
public interface ParseTree
{

    public void replace( ParseTree replacement )
        throws ParseTreeException;

    /**
     * Makes a new copy (another object) of this nonleaf-node recursively.
     * The objects contained by this object will also be copied.
     *
     * @return  the copy of this nonleaf-node as a ptree-node.
     */
    public ParseTree makeRecursiveCopy();

    /**
     * Makes a new copy of this nonleaf-node as a ptree-node.
     * The objects contained by the new object are same as
     * these contained by the original object.
     *
     * @return  the copy of this nonleaf-node as a ptree-node.
     */
    public ParseTree makeCopy();

    /**
     * Tests if this parse-tree-node's value equals to the specified
     * ptree-node's.
     *
     * @return  true if two values are same.
     */
    public boolean equals( ParseTree p );

    /**
     * Tests if this parse-tree-node's value equals to the specified
     * ptree-node's.
     *
     * @return  true if two this and p refer to the same ptree object.
     */
    public boolean eq( ParseTree p );

    /**
     * Generates string which presents for this parse-tree
     *
     * @return string which presents for this parse-tree
     */
    public String toString();

    /**
     * @deprecated
     */
    public void writeCode();

    /**
     * Generates the string expression from this node.  Returned
     * string doesn't have '"' without cancel ( \" ) and doesn't have
     * newline.<p>
     *
     * This method is useful to embed ptree objects as a string literal
     * in source code.
     *
     * @return the flatten string which this node represents
     */
    public String toFlattenString();

    /**
     * Returns the Identifier Number of this object
     *
     * @return the ID number of this object
     */
    public int getObjectID();

    /**
     * Accepts a <code>ParseTreeVisitor</code> object as the role of a
     * Visitor in the Visitor pattern, as the role of an Element in the
     * Visitor pattern.<p>
     * 
     * This invoke an appropriate <code>visit()</code> method on the
     * accepted visitor.
     * 
     * @param visitor a visitor
     */
    public void accept( ParseTreeVisitor visitor )
	throws ParseTreeException;

    /**
     * Accepts a <code>ParseTreeVisitor</code> object as the role of a
     * Visitor in the Visitor pattern, as the role of an Element in the
     * Visitor pattern.<p>
     *
     * This invoke an appropriate <code>visit()</code> method on each
     * child <code>ParseTree</code> object with this visitor.
     * 
     * @param visitor a visitor
     */
    public void childrenAccept( ParseTreeVisitor visitor )
	throws ParseTreeException;

}
