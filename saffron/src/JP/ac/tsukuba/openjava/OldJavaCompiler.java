/*
 * OldJavaCompiler.java
 *
 * Apr 16, 1999  Michiaki Tatsubori
 */
package JP.ac.tsukuba.openjava;


import java.io.*;
import openjava.ojc.JavaCompiler;
import openjava.ojc.JavaCompilerArgs;


/**
 * The class <code>OldJavaCompiler</code> is an adapter for Sun's javac.
 *
 */
public class OldJavaCompiler implements JavaCompiler
{
	JavaCompilerArgs args = new JavaCompilerArgs();

    public static void main( String[] args ) {
		OldJavaCompiler compiler = new OldJavaCompiler();
		compiler.args.setStringArray(args);
		compiler.compile();
    }

	public JavaCompilerArgs getArgs() {
		return args;
	}

	public ClassLoader getClassLoader() {
		return null;
	}

	public void compile() {
		String[] args = this.args.getStringArray();
        /*sun.tools.javac.Main.main( args );*/
        Runtime runtime = Runtime.getRuntime();
	try {
	    Process p = runtime.exec( "javac " + strs2str( args ) );
	    InputStream in = new BufferedInputStream( p.getErrorStream() );
	    byte[] buf = new byte[1024];
	    for (int len = in.read( buf ); len != -1; len = in.read( buf ) ) {
	        System.err.write( buf, 0, len );
	    }
	    p.waitFor();
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    private static String strs2str( String[] strs ) {
        StringBuffer buf = new StringBuffer();
	for (int i = 0; i < strs.length; ++i) {
	    buf.append( strs[i] ).append( " " );
	}
	return buf.toString();
    } 
       
}
