/*
 * ParseTreeObject.java 1.0
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


import openjava.ptree.util.ParseTreeVisitor;
import openjava.ptree.util.SourceCodeWriter;
import openjava.tools.WriterStack;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.StringTokenizer;


/**
 * The ParseTree class presents for the node of parse tree.
 * This may be a token node, Leaf, or may be a nonterminal node, NonLeaf.
 *
 * @see openjava.ptree.Leaf
 * @see openjava.ptree.NonLeaf
 */
public abstract class ParseTreeObject extends Object
    implements ParseTree, Cloneable
{
    // Why this modifier is not final ?
    // Because of javac bug in excuting it with -O option.
    protected static String LN;
    static{
        StringWriter strw = new StringWriter();
        PrintWriter pw = new PrintWriter( strw );
        pw.println();
        pw.close();
        LN = strw.toString();
    }

    private ParseTreeObject parent;
    /*************/
    public /*private*/ final ParseTreeObject getParent() {
        return parent;
    }
    protected final void setParent( ParseTreeObject parent ) {
        this.parent = parent;
    }
    public final void replace( ParseTree replacement )
        throws ParseTreeException
    {
        ParseTreeObject p = getParent();
        if (p == null)  throw new ParseTreeException( "no parent" );
        p.replaceChildWith( this, replacement );
    }
    protected abstract void
    replaceChildWith( ParseTree dist, ParseTree replacement )
        throws ParseTreeException;

    /**
     * Arrocates new parse-tree object and set identifier number
     * on the object.
     *
     */
    public ParseTreeObject() {
        setObjectID();
    }

    /**
     * clone() is fixed as a shallow copy.
     *
     * @return  the copy of this ptree-node.
     */
    protected final Object clone() {
        try {
            ParseTreeObject result = (ParseTreeObject) super.clone();
            result.setObjectID();
            return result;
        } catch ( CloneNotSupportedException e ) {
            return null;
        }
    }

    /**
     * shallow copy
     */
    public ParseTree makeCopy() {
        return (ParseTree) clone();
    }

    /**
     * deep copy
     */
    public abstract ParseTree makeRecursiveCopy();

    /**
     * Tests if this parse-tree-node's value equals to the specified
     * ptree-node's.
     *
     * @return  true if two values are same.
     */
    public abstract boolean equals( ParseTree p );

    /**
     * Tests if this parse-tree-node's value equals to the specified
     * ptree-node's.
     *
     * @return  true if two this and p refer to the same ptree object.
     */
    public final boolean eq( ParseTree p ){
      return eq( this, p );
    }

    public int hashCode() {
        return getObjectID();
    }

    /**
     * Generates a string object of regular Java source code
     * representing this parse-tree.
     *
     * @return string which represents this parse-tree
     */
    public String toString() {
        StringWriter strwriter = new StringWriter();
        PrintWriter out = new PrintWriter( strwriter );
        SourceCodeWriter writer = new SourceCodeWriter( out );
        try {
            this.accept( writer );
        } catch ( ParseTreeException e ) {
            System.err.println( "fail in toString()" );
        }
        out.close();
        return strwriter.toString();
    }

    /** this is used as like grobal variable in ParseTree class-family. */
    protected static WriterStack writerStack = new WriterStack();

    /** this is used as like grobal variable in ParseTree class-family. */
    protected static PrintWriter out = null;
    static{
        out = getPrintWriter();
    }

    /**
     * Sets the writer to the one from the specified stream.
     *
     * @param  pstream  print stream
     */
    public static void setPrintStream( PrintStream pstream ) {
      writerStack.push( pstream );
      // for compatibility
      out = getPrintWriter();
    }

    public static PrintWriter getPrintWriter() {
      return writerStack.peek();
    }

    public static PrintWriter popPrintWriter() {
      getPrintWriter().flush();
      PrintWriter pw = writerStack.pop();
      // for compatibility
      out = getPrintWriter();

      return pw;
    }

    public static void pushPrintWriter( PrintWriter pw) {
      if(!writerStack.empty()){
        getPrintWriter().flush();
      }
      writerStack.push( pw );
      // for compatibility
      out = getPrintWriter();
    }

    public static void flushPrintWriter() {
      getPrintWriter().flush();
    }

    /** to write debugging code */
    protected static boolean debugFlag = true;
    protected static int debugLevel = 0;

    /** to write debugging code */
    private static String tab = "  ";

    /** to write debugging code */
    private static int nest = 0;

    public static void setDebugFlag( boolean df ) {
      debugFlag = df;
    }

    public static void setDebugLevel( int n ) {
      debugLevel = n;
    }

    public static boolean getDebugFlag() {
      return debugFlag;
    }

    public static void setTab( String str ) {
      ParseTreeObject.tab = str;
    }

    public static String getTab() {
      return ParseTreeObject.tab;
    }

    public static void setNest( int i )
    {
      nest = i;
    }

    public static int getNest()
    {
      return nest;
    }

    public static void pushNest()
    {
      setNest( getNest() + 1 );
    }

    public static void popNest()
    {
      setNest( getNest() - 1 );
    }

    protected void writeDebugL() {
      if(debugFlag == true){
        getPrintWriter().print( "[" );
        if(debugLevel > 0){
        String qname = this.getClass().getName();
        String sname = qname.substring( qname.lastIndexOf( '.' ) + 1 );
        getPrintWriter().print( sname + "#" );
        if(debugLevel > 1){
          getPrintWriter().print( getObjectID() );
        }
        getPrintWriter().print( " " );
        }
      }
    }

    protected static void writeDebugR() {
      if(debugFlag == true) getPrintWriter().print( "]" );
    }

    protected static void writeDebugLR() {
      if(debugFlag == true) getPrintWriter().print( "[]" );
    }

    protected static void writeDebugLln() {
      if(debugFlag == true) getPrintWriter().println( "[" );
    }

    protected static void writeDebugRln() {
      if(debugFlag == true) getPrintWriter().println( "]" );
    }

    protected static void writeDebugln() {
      if(debugFlag == true) getPrintWriter().println();
    }

    protected static void writeDebug( String str ) {
      if(debugFlag == true) getPrintWriter().print( str );
    }

    protected static void writeTab() {
      for( int i = 0; i < nest; i++ ){
        getPrintWriter().print( getTab() );
      }
    }

    /**
     * Use accept(SourceCodeWriter).
     * @deprecated
     */
    public abstract void writeCode() ;

    /**
     * Return true if only they refer to the same object.
     * If two objects are both null, this returns true.
     *
     * @param p the ptree-node
     * @param q the ptree-node
     * @return true if p and q refer to same object.
     */
    public static final boolean eq( ParseTree p, ParseTree q ) {
        if(p == null && q == null)  return true;
        if(p == null || q == null)  return false;
        return (p.getObjectID() == q.getObjectID());
    }

    /**
     * May return true if two ptree-nodes don't refer to not the
     * same objects but their contents are equivalent.
     *
     * @param p the node
     * @param q the node to compare
     * @return true if p equals c
     */
    public static final boolean equal( ParseTree p, ParseTree q ) {
        if(p == null && q == null)  return true;
        if(p == null || q == null)  return false;
        if(eq( p, q ))  return true;
        return p.equals( q );
    }

    /**
     * The object ID is object identifier number in order to determine
     * two ptree variables refer to the same object.
     */
    private int objectID = -1;

    /** The idCount counts up generated ptree objects */
    private static int idCount = 0;

    public static final int lastObjectID() {
        return idCount;
    }

    /**
     * Increments idCount class variable and set the instance's
     * object identifier number to the number of idCount variable.
     *
     */
    private synchronized final void setObjectID() {
        idCount++;
        objectID = idCount;
    }

    /**
     * Returns name the ID represents
     *
     * @return Name the ID represents
     */
    public final int getObjectID() {
        return objectID;
    }

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
    public String toFlattenString() {
        StringBuffer ret = null;

        /* cancel double quotes and back slashs */
        java.util.StringTokenizer canceller
            = new StringTokenizer( this.toString(), "\\\"", true );
        ret = new StringBuffer();

        while (canceller.hasMoreTokens()) {
            String buf = canceller.nextToken();
            if (buf.equals( "\\" ) || buf.equals( "\"" )) {
                ret.append( "\\" );
            }
            ret.append( buf );
        }

        /* remove new line chars */
        java.util.StringTokenizer lnremover
            = new StringTokenizer( ret.toString(), "\n\r", false );
        ret = new StringBuffer();

        while (lnremover.hasMoreTokens()) {
            ret.append( " " + lnremover.nextToken() );
        }

        return ret.toString();
    }

    /**
     * Accepts a <code>ParseTreeVisitor</code> object as the role of a
     * Visitor in the Visitor pattern, as the role of an Element in
     * the Visitor pattern.<p>
     *
     * This invoke an appropriate <code>visit()</code> method on the
     * accepted visitor.
     *
     * @param visitor a visitor
     */
    public abstract void accept( ParseTreeVisitor visitor )
        throws ParseTreeException;

    /*
    public void accept( ParseTreeVisitor visitor ) throws ParseTreeException {
        Class active_type = this.getClass();
        Class visitor_clazz = visitor.getClass();
        try {
            Method visit_method
                = visitor_clazz.getMethod( "visit",
                                           new Class[]{ active_type } );
            visit_method.invoke( visitor, new Object[]{ this } );
        } catch ( Exception e ) {
            System.err.println( e.toString() );
            System.err.println( "\telement\t-" + active_type.toString() );
            System.err.println( "\tvisitor\t-" + visitor_clazz.toString() );
        }
    }
    */

    /**
     * Accepts a <code>ParseTreeVisitor</code> object as the role of a
     * Visitor in the Visitor pattern, as the role of an Element in the
     * Visitor pattern.<p>
     *
     * This invoke an appropriate <code>visit()</code> method on each
     * child <code>ParseTree</code> object with this visitor.
     *
     * @param visitor a visitor
     **/
    public abstract void childrenAccept( ParseTreeVisitor visitor )
        throws ParseTreeException;

}
