/*
 * $Id$
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


/**
 * Syntax rule concerning lists of expressions.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see ExpressionList
 */
public final class ExpressionListRule extends SeparatedListRule
{
    private ExpressionList exprList = null;

    public ExpressionListRule( ExpressionRule exprRule, boolean allowsEmpty ) {
        super( exprRule, TokenID.COMMA, allowsEmpty );
    }

    public ExpressionListRule( ExpressionRule exprRule ) {
        this( exprRule, false );
    }

    public ExpressionListRule( Environment env, boolean allowsEmpty ) {
        this( new ExpressionRule( env ), allowsEmpty );
    }

    public ExpressionListRule( Environment env ) {
        this( env, false );
    }

    protected void initList() {
        exprList = new ExpressionList();
    }

    protected void addListElement( Object elem ) {
        exprList.add( (Expression) elem );
    }

    protected ParseTree getList() {
        return exprList;
    }

}
