/*
 * $Id$
 *
 * Initilializing must be done after loading classes.
 *
 * Jul 28, 1998 modified by mich
 */
package openjava.mop;


import openjava.tools.DebugOut;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Hashtable;


public class GlobalEnvironment
        extends Environment
{
    private Hashtable table = new Hashtable();

    public GlobalEnvironment() {
    }

    public String toString() {
        StringWriter str_writer = new StringWriter();
        PrintWriter out = new PrintWriter( str_writer );

	out.println( "GlobalEnvironment" );
        out.print( "class object table : " );
        out.println( table.toString() );

        out.flush();
        return str_writer.toString();
    }

    /**
     * Gets the package name.
     */
    public String getPackageName()
    {
        return null;
    }

    /**
     * Looks a class object up.
     *
     * @param	name		the name of the fully-qualified name of
     *				the class looked for
     */
    public OJClass lookupClass( String name ) {
        if (name == null)  return null;
        return (OJClass) table.get( name );
    }

    /**
     * Records a class object.
     *
     * @param	name		the fully-qualified name of the class
     * @param	clazz		the class object associated with that name
     */
    public void record( String name, OJClass clazz ) {
        String str = null;
        try {
            str = clazz.toString();
        } catch (Exception ex) {
            str = "<" + ex.toString() + ">";
        }
	DebugOut.println("Genv#record(): " + name + " " + str);
	table.put(name, clazz);
    }

    /**
     * Obtains the fully-qualified name of the given class name.
     *
     * @param  name  a simple class name or a fully-qualified class name
     * @return  the fully-qualified name of the class
     */
    public String toQualifiedName( String name ) {
        return name;
    }

    /**
     * binds a name to the class type.
     *
     * @param     name            the fully-qualified name of the class
     * @param     info           the class object associated with that name
     */
    public void bindVariable( String name, VariableInfo info ) {
	System.err.println( "error : illegal binding on GlobalEnvironment" );
    }

    public VariableInfo lookupBind( String name ) {
	return null;
    }

}
