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


import openjava.ptree.*;


/**
 * Simple syntax rule for handling lists.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public final class DefaultListRule extends SeparatedListRule
{
    private ObjectList list = null;

    public DefaultListRule( SyntaxRule elementRule,
                            int separator_token, boolean allowsEmpty ) {
        super( elementRule, separator_token, allowsEmpty );
    }

    public DefaultListRule( SyntaxRule elementRule, int separator_token ) {
        this( elementRule, separator_token, false );
    }

    protected void initList() {
        list = new ObjectList();
    }

    protected void addListElement( Object elem ) {
        list.add( elem );
    }

    protected ParseTree getList() {
        return list;
    }

}
