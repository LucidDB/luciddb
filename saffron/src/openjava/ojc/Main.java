/*
 * Main.java
 *
 *
 */
package openjava.ojc;


import java.io.*;
import java.util.Vector;
import java.util.Hashtable;
import java.lang.reflect.Constructor;
import openjava.tools.parser.*;
import openjava.tools.DebugOut;
import openjava.mop.*;
import openjava.ptree.*;
import openjava.ptree.util.*;


public class Main
{

    public static void main( String argc[] ) {
        System.err.println( "OpenJava Compiler Version 1.0a " +
			    "build 20011117" );
        CommandArguments arguments;
	try {
	    arguments = new CommandArguments( argc );
	} catch (Exception e) {
	    showUsage();
	    return;
	}
	if (arguments.getOption( "gui" ) != null) {
	    new GUICompiler( arguments ).run();
	} else {
	    new Compiler( arguments ).run();
	}
    }

    private static void showUsage() {
        PrintStream o = System.err;
        o.println( "Usage : ojc <options> <source files>" );
        o.println( "where <options> includes:" );
        o.println( "  -verbose                 " +
		   "Enable verbose output                  " );
        o.println( "  -g=<number>              " +
		   "Specify debugging info level           " );
        o.println( "  -d=<directory>           " +
		   "Specify where to place generated files " );
	o.println( "  -compiler=<class>        " +
		   "Specify regular Java compiler          " );
	o.println( "  --default-meta=<file>    " +
		   "Specify separated meta-binding configurations" );
	o.println( "  -calleroff               " +
		   "Turn off caller-side translations      " );
	o.println( "  -C=<argument>            " +
		   "Pass the argument to Java compiler     " );
	o.println( "  -J<argument>             " +
		   "Pass the argument to JVM               " );
    }

}
