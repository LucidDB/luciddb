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
package openjava.mop;


import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Hashtable;
import openjava.tools.DebugOut;


/**
 * An environment whose symbols come from local Java variable, method and class
 * declarations. It inherits more symbols from its parent environment.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class ClosedEnvironment extends Environment
{
    protected Hashtable table = new Hashtable();
    protected Hashtable symbol_table = new Hashtable();

    /**
     * Creates a ClosedEnvironment.
     */
    public ClosedEnvironment( Environment env ) {
        parent = env;
    }

    public String toString() {
        StringWriter str_writer = new StringWriter();
        PrintWriter out = new PrintWriter( str_writer );

        out.println( "ClosedEnvironment" );
        out.println( "class object table : " + table );
        out.println( "binding table : " + symbol_table );
        out.println( "parent env : " + parent );

        out.flush();
        return str_writer.toString();
    }

    public void record( String name, OJClass clazz ) {
        DebugOut.println( "ClosedEnvironment#record() : "
                         + name + " "  + clazz.getName() );
        Object result = table.put( name, clazz );
        if (result != null) {
          System.err.println( name + " is already binded on "
                             + result.toString() );
        }
    }

    public OJClass lookupClass( String name ) {
        name = name.replace('$','.');
        OJClass result = (OJClass) table.get( name );
        if (result != null)  return result;
        return parent.lookupClass( name );
    }

    /**
     * binds a name to the class type.
     *
     * @param     name            the fully-qualified name of the class
     * @param     info           the class object associated with that name
     */
    public void bindVariable( String name, VariableInfo info ) {
                symbol_table.put( name, info );
    }

    public VariableInfo lookupBind( String name ) {
        VariableInfo info = (VariableInfo) symbol_table.get( name );
        if (info != null)  return info;
        if (parent == null)  return null;
        return parent.lookupBind( name );
    }

}
