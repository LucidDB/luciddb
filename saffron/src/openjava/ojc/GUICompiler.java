/*
 * GUICompiler.java
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
import openjava.debug.gui.*;


public class GUICompiler extends Compiler
{
    private final Object lock = new Object();

    GUICompiler( CommandArguments arguments ) {
	super( arguments );
    }

    private int callee_min = 0xfffffff;
    private int caller_min = 0xfffffff;


    public void run() {
	/* first parsing */
        FileEnvironment[] file_env = new FileEnvironment[files.length];
        CompilationUnit[] comp_unit = new CompilationUnit[files.length];

        System.err.println( "Generating parse tree." );
	generateParseTree( file_env, comp_unit );
        System.err.println( "Successfully parsed." );

	initDebug();
        System.err.println();

        System.err.println( "Initializing parse tree." );
	initParseTree( file_env, comp_unit );
        System.err.println( "Successfully initialized." );

	callee_min = ParseTreeObject.lastObjectID();
	outputToDebugFile( file_env, comp_unit, 0 );
        System.err.println();

        System.err.println( "Translating callee side" );
	translateCalleeSide( file_env, comp_unit );
        System.err.println( "..done." );

	caller_min = ParseTreeObject.lastObjectID();
	outputToDebugFile( file_env, comp_unit, 1 );
        System.err.println();

        System.err.println( "Translating caller side" );
	translateCallerSide( file_env, comp_unit );
        System.err.println( "..done." );

	generateAdditionalCompilationUnit();
        System.err.println();

        System.err.println( "Printing parse tree." );
	outputToFile( file_env, comp_unit );
        System.err.println( "Successfully printed." );

	outputToDebugFile( file_env, comp_unit, 2 );
        System.err.println();

	/*System.err.println( "Compiling into bytecode." );
	javac( file_env, comp_unit );*/
        /*System.err.println( "Successfully compiled." );*/

	System.err.flush();
    }

//    private SourceCodeViewer sviewer;
    void initDebug() {
//        sviewer = new SourceCodeViewer( lock );
    }

    void outputToDebugFile( FileEnvironment[] fenv,
			    CompilationUnit[] comp_unit,
			    int num )
    {
	synchronized( lock ) {
	    try {
		lock.wait();
	    } catch ( InterruptedException e ) {
		e.printStackTrace();
	    }

            try {
                ColoredSourceWriter writer
		    = new ColoredSourceWriter( null /*sviewer.getEntry( num )*/,
					       callee_min, caller_min );
                writer.setDebugLevel( arguments.getDebugLevel() );
                comp_unit[0].accept( writer );
            } catch ( Exception e ) {
                e.printStackTrace();
            }
        }
    }

}
