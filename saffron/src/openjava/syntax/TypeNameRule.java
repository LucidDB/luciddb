/*
 * TypeNameRule.java
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


import java.lang.Object;
import openjava.tools.parser.*;
import openjava.ptree.*;
import openjava.mop.Environment;


/**
 * Syntax rule concerning type names.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see TypeName
 */
public class TypeNameRule extends AbstractSyntaxRule
{
    public final ParseTree consume( TokenSource token_src )
        throws SyntaxException
    {
        return consumeTypeName( token_src );
    }

    public TypeName consumeTypeName( TokenSource token_src )
        throws SyntaxException
    {
        TypeName result = JavaSyntaxRules.consumeTypeName( token_src );
        if (result == null)  throw JavaSyntaxRules.getLastException();
        Environment env = token_src.getEnvironment();
        result.setName( env.toQualifiedName( result.getName() ) );
        return result;
    }

}
