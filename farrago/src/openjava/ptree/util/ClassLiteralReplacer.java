/*
 * ClassLiteralReplacer.java
 *
 * Make typenames qualified.
 * <p>
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1998 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.ptree.util;


import openjava.mop.*;
import openjava.ptree.*;

/**
 * Replaces occurrences of <code>foo.Bar.class</code> with
 * <code>Class.forName("foo.Bar")</code>.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class ClassLiteralReplacer extends EvaluationShuttle
{
    public static final String OLDCLASS_PREFIX = "oldjavaclass.";

    public ClassLiteralReplacer( Environment env ) {
        super( env );
    }

    public Expression evaluateDown( ClassLiteral ptree )
        throws ParseTreeException
    {
        TypeName type = ptree.getTypeName();

        if (type.toString().startsWith( OLDCLASS_PREFIX )) {
            String name = type.getName();
            name = name.substring( OLDCLASS_PREFIX.length() );
            int dim = type.getDimension();
            return new ClassLiteral( new TypeName( name, dim ) );
        }

        ExpressionList args = new ExpressionList( new ClassLiteral( type ) );
        Expression result
            = new MethodCall( OJClass.forClass( OJClass . class ),
                              "forClass", args );
        return result;
    }

}

