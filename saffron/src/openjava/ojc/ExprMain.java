/*
 * ExprMain.java
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


public class ExprMain
{

    public static void main( String argc[] ) {
        System.err.println( "OpenJava Compiler Version 1.0a1 " +
			    "build 19991022" );
        CommandArguments arguments;
	try {
	    arguments = new CommandArguments( argc );
	} catch (Exception e) {
	    showUsage();
	    return;
	}

	ExprCompiler compiler = new ExprCompiler( arguments );

	long total = 0, begin, end;
	{
	    System.err.println( "Cold Start" );
	    begin = System.currentTimeMillis();
	    compiler.run();
	    end  = System.currentTimeMillis();
	    System.err.println( end - begin );
	}
	{
	    System.err.println( "Hot Start" );
	    begin = System.currentTimeMillis();
	    compiler.run();
	    end  = System.currentTimeMillis();
	    System.err.println( end - begin );
	}
	{
	    System.err.println( "Hot Start" );
	    begin = System.currentTimeMillis();
	    compiler.run();
	    end  = System.currentTimeMillis();
	    System.err.println( end - begin );
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
	o.println( "  -C=<argument>            " +
		   "Pass the argument to Java compiler     " );
	o.println( "  -J<argument>             " +
		   "Pass the argument to JVM               " );
    }

}
