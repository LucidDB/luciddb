/*
 * $Id$
 *
 * comments here.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1998 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.syntax;


import openjava.mop.Environment;
import openjava.tools.parser.*;
import openjava.ptree.*;
import openjava.ptree.util.*;


/**
 * Utility methods for syntax rules.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class JavaSyntaxRules implements TokenID
{
    private JavaSyntaxRules() {}

    private static ParseException lastException = null;
    public static SyntaxException getLastException() {
        return new SyntaxException( lastException );
    }

    private static ParseTree correct( ParseTree ptree, Environment env ) {
        MemberAccessCorrector corrector
            = new MemberAccessCorrector( env );
        ObjectList holder = new ObjectList( ptree );
        try {
            ptree.accept( corrector );
        } catch ( ParseTreeException e ) {
            System.err.println( e.getMessage() );
        }
        return (ParseTree) holder.get( 0 );
    }

    private static void
    adjustTokenSource( TokenSource token_src, Token last_token ) {
        Token token = token_src.getToken( 0 );
        while (token != last_token) {
            token = token_src.getNextToken();
        }
        return;
    }

    /**
     * Consumes a single expression from given token source.
     *
     * @param token_src token source
     * @param env environment
     * @return  null if this fail, otherwise an expression.
     */
    public static final Expression
    consumeExpression( TokenSource token_src, Environment env ) {
        if (env == null)  env = token_src.getEnvironment();
        RestorableTokenSource dummy = new RestorableTokenSource( token_src );
        Parser parser = new Parser( dummy );
        try {
            Expression ptree = parser.Expression( env );
            adjustTokenSource( token_src, parser.getToken( 0 ) );
            return (Expression) correct( ptree, env );
        } catch ( ParseException e ) {
            lastException = e;
            return null;
        }
    }
    public static final Expression
    consumeExpression( TokenSource token_src ) {
        return consumeExpression(  token_src, null );
    }

    /**
     * Consumes a statement.
     * This consumes only a pure statement excluding local variable
     * declarations and local class declarations.
     *
     * @param token_src token source
     * @return  null if this fail, otherwise a statement.
     */
    public static final Statement
    consumeStatement( TokenSource token_src, Environment env ) {
        if (env == null)  env = token_src.getEnvironment();
        RestorableTokenSource dummy = new RestorableTokenSource( token_src );
        Parser parser = new Parser( dummy );
        try {
            Statement ptree = parser.Statement( env );
            adjustTokenSource( token_src, parser.getToken( 0 ) );
            return (Statement) correct( ptree, env );
        } catch ( ParseException e ) {
            lastException = e;
            return null;
        }
    }
    public static final Statement consumeStatement( TokenSource token_src ) {
        return consumeStatement(  token_src, token_src.getEnvironment() );
    }

    /**
     * Consumes a block.
     *
     * @param token_src token source
     * @return  null if this fail, otherwise a block.
     */
    public static final Block
    consumeBlock( TokenSource token_src, Environment env ) {
        if (env == null)  env = token_src.getEnvironment();
        RestorableTokenSource dummy = new RestorableTokenSource( token_src );
        Parser parser = new Parser( dummy );
        try {
            Block ptree = parser.Block( env );
            adjustTokenSource( token_src, parser.getToken( 0 ) );
            return (Block) correct( ptree, env );
        } catch ( ParseException e ) {
            lastException = e;
            return null;
        }
    }
    public static final Block consumeBlock( TokenSource token_src ) {
        return consumeBlock(  token_src, token_src.getEnvironment() );
    }

    /**
     * Consumes a type name.
     * Including primitive type and reference type.
     *
     * @param token_src token source
     * @return  null if this fail, otherwise a block.
     */
    public static final TypeName
    consumeTypeName( TokenSource token_src ) {
        Environment env = token_src.getEnvironment();
        RestorableTokenSource dummy = new RestorableTokenSource( token_src );
        Parser parser = new Parser( dummy );
        try {
            TypeName ptree = parser.Type( env );
            adjustTokenSource( token_src, parser.getToken( 0 ) );
            return (TypeName) correct( ptree, env );
        } catch ( ParseException e ) {
            lastException = e;
            return null;
        }
    }

}
