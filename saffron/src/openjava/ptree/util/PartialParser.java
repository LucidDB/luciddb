/*
 * PartialParser.java 1.0
 *
 * This class can be used to make ptree objects from string.
 *
 * Jun 11, 1997 mich
 * Oct 17, 1997 mich
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.ParseTreeObject
 * @version 1.0 last updated: Oct 17, 1997
 * @author  Michiaki Tatsubori
 */
package openjava.ptree.util;


import java.io.*;
import openjava.tools.DebugOut;
import openjava.tools.parser.Parser;
import openjava.mop.*;
import openjava.ptree.*;


/**
 * The <code>PartialParser</code> class is
 * an utilty class to make ptree objects from string.
 *
 * @see openjava.ptree.ParseTree
 * @see openjava.ptree.Expression
 * @see openjava.ptree.Statement
 * @see openjava.ptree.StatementList
 * @see openjava.ptree.MemberDeclaration
 * @see openjava.ptree.MemberDeclarationList
 * @see openjava.ptree.ClassDeclaration
 * @see openjava.ptree.CompilationUnit
 */
public final class PartialParser
{
    /**
     * Constructor should not be called.
     *
     */
    protected PartialParser() {
    }

    private static ParseTree initialize( Environment env, ParseTree p )
        throws ParseTreeException
    {
        /* dummy wrapper in case replacement occurs */
        ObjectList holder = new ObjectList( p );
        p.accept( new MemberAccessCorrector( env ) );
        return (ParseTree) holder.get( 0 );
    }

    /**
     * Replaces "#s" "#EXPR" "#STMT" "#STMTS" in the given string with
     * the given arguments.
     * <p>
     * "##" is replaced with "#".
     * <p>
     * <br><blockquote><pre>
     *   #s      arg.toString()
     *   #EXPR   ((Expression) arg).toString()
     *   #STMT   ((Statement) arg).toString()
     *   #STMTS  ((StatementList) arg).toString()
     * </pre></blockquote><br>
     *
     */
    public static final String
    replace( String base, Object[] args ) throws MOPException {
        try {
            StringBuffer result = new StringBuffer();

            int arg_i = 0, index = 0, found;
            while ((found = base.indexOf( '#', index )) != -1) {
                result.append( base.substring( index, found ) );
                if (base.regionMatches( found, "#STMTS", 0, 6 )) {
                    result.append( args[arg_i++] );
                    index = found + 6;
                } else if (base.regionMatches( found, "#STMT", 0, 5 )) {
                    result.append( args[arg_i++] );
                    index = found + 5;
                } else if (base.regionMatches( found, "#EXPR", 0, 5 )) {
                    result.append( args[arg_i++] );
                    index = found + 5;
                } else if (base.regionMatches( found, "#s", 0, 2 )) {
                    result.append( args[arg_i++].toString() );
                    index = found + 2;
                } else if (base.regionMatches( found, "##", 0, 2 )) {
                    result.append( '#' );
                    index = found + 2;
                } else {
                  result.append( '#' );
                  index = found + 1;
                }
            }
            result.append( base.substring( index ) );

            return result.toString();
        } catch ( Exception e ) {
            /* special exception is better */
            throw new MOPException( "PartialParser.replace() : " +
                                    "illegal format for arguments : " +
                                    base );
        }
    }

    public static final String
    replace( String base, Object a0 ) throws MOPException {
        return replace( base, new Object[]{ a0 } );
    }

    public static final String
    replace( String base, Object a0, Object a1 )
        throws MOPException
    {
        return replace( base, new Object[]{ a0, a1 } );
    }

    public static final String
    replace( String base, Object a0, Object a1, Object a2 )
        throws MOPException
    {
        return replace( base, new Object[]{ a0, a1, a2 } );
    }

    public static final String
    replace( String base, Object a0, Object a1, Object a2, Object a3 )
        throws MOPException
    {
        return replace( base, new Object[]{ a0, a1, a2, a3 } );
    }

    public static final String
    replace( String base, Object a0, Object a1, Object a2, Object a3,
             Object a4 )
        throws MOPException
    {
        return replace( base, new Object[]{ a0, a1, a2, a3, a4 } );
    }

    public static final String
    replace( String base, Object a0, Object a1, Object a2, Object a3,
             Object a4, Object a5 )
        throws MOPException
    {
        return replace( base, new Object[]{ a0, a1, a2, a3, a4, a5 } );
    }

    public static final String
    replace( String base, Object a0, Object a1, Object a2, Object a3,
             Object a4, Object a5, Object a6 )
        throws MOPException
    {
        return replace( base, new Object[]{ a0, a1, a2, a3, a4, a5, a6 } );
    }

    public static final String
    replace( String base, Object a0, Object a1, Object a2, Object a3,
             Object a4, Object a5, Object a6, Object a7 )
        throws MOPException
    {
        return replace( base, new Object[]{ a0, a1, a2, a3, a4, a5, a6, a7 } );
    }

    /**
     * Converts a string into an expression. For example:
     * <br><blockquote><pre>
     *     "i + 3"
     * </pre></blockquote><br>
     * or :
     * <br><blockquote><pre>
     *     "f()"
     * </pre></blockquote><br>
     *
     * @return  the expression node which the specified string
     *          represents.
     * @exception  MOPException  if any critical error occurs.
     */
    public static Expression makeExpression( Environment env, String str )
        throws MOPException
    {
        DebugOut.println( "PP makeExpression() : " + str );
        Parser parser = new Parser( new StringReader( str ) );
        Expression result;
        try {
            result = parser.Expression( env );
            result = (Expression) initialize( env, result );
        } catch (Exception e) {
            if (false) {
                System.err.println( "partial parsing failed for : " + str );
                System.err.println( e );
                System.err.println( env.toString() );
            }
            throw new MOPException( e );
        }
        return result;
    }

    /**
     * Converts a string into a statement. For example:
     * <br><blockquote><pre>
     *     "i++;"
     * </pre></blockquote><br>
     * or :
     * <br><blockquote><pre>
     *     "for(;;){ f(); }"
     * </pre></blockquote><br>
     * <p>
     * But local variable declarations are not allowed.
     *
     * @return  the statement node which the specified string
     *          represents.
     * @exception  MOPException  if any critical error occurs.
     */
    public static Statement makeStatement( Environment env, String str )
        throws MOPException
    {
        DebugOut.println( "PP makeStatement() : " + str );
        Parser parser = new Parser( new StringReader( str ) );
        Statement result;
        try {
            result = parser.Statement( env );
            result = (Statement) initialize( env, result );
        } catch (Exception e) {
            if (false) {
                System.err.println( "partial parsing failed for : " + str );
                System.err.println( e );
                System.err.println( env.toString() );
            }
            throw new MOPException( e );
        }
        return result;
    }

    /**
     * Converts a string into a statement list. For example:
     * <br><blockquote><pre>
     *     "i++; j = 3;"
     * </pre></blockquote><br>
     * <p>
     * Local variable declarations like following can also be parsed.
     * <br><blockquote><pre>
     *     "int  n, m;"
     * </pre></blockquote><br>
     *
     * @return  the statements node which the specified string
     *          represents.
     * @exception  MOPException  if any critical error occurs.
     */
    public static StatementList makeStatementList( Environment env, String str )
        throws MOPException
    {
        DebugOut.println( "PP makeStatementList() : " + str );
        Parser parser = new Parser( new StringReader( str ) );
        env = new ClosedEnvironment( env );
        StatementList result;
        try {
            result = parser.BlockOrStatementListOpt( env );
            result = (StatementList) initialize( env, result );
        } catch (Exception e) {
            if (false) {
                System.err.println( "partial parsing failed for : " + str );
                System.err.println( e );
                System.err.println( env.toString() );
            }
            throw new MOPException( e );
        }
        return result;
    }

}
