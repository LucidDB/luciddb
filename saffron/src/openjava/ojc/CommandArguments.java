/*
 * CommandArguments.java
 *
 * comments here.
 *
 * @author   Michiaki Tatsubori
 * @version  %VERSION% %DATE%
 * @see      java.lang.Object
 *
 * COPYRIGHT 1998 by Michiaki Tatsubori, ALL RIGHTS RESERVED.
 */
package openjava.ojc;


import java.util.Hashtable;
import java.util.Vector;
import java.io.*;


/**
 * Represents the argument list passed to a Java compiler.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see JavaCompiler
 * @see JavaCompilerArgs
 */
public class CommandArguments
{
    public static final int DEBUG_VERBOSE = 0x4;
    public static final int DEBUG_CALLER = 0x8;

    private final String[] originalArgs;
    private Hashtable options = new Hashtable();
    private File[] files;

    public CommandArguments( String args[] ) throws IOException {
        originalArgs = args;
        files = initFiles();
        checkArguments();
    }

    public File[] getFiles() {
        return files;
    }

    public File[] initFiles() {
        if (originalArgs == null)  return new File[0];

        File[] result;
        int file_num = originalArgs.length - countUpOptions( originalArgs );
        result = new File[file_num];

        for (int i = 0, count = 0; i < originalArgs.length; ++i) {
            if (isOption( originalArgs[i] )) {
                registerOption( originalArgs[i].substring( 1 ) );
            } else {
                result[count++] = new File( originalArgs[i] );
            }
        }

        return result;
    }

    private static boolean isOption( String arg ) {
        return (arg != null && arg.charAt(0) == '-');
    }

    private static int countUpOptions( String[] args ) {
        if (args == null)  return 0;
        int result = 0;
        for (int i = 0; i < args.length; ++i) {
            if ( isOption( args[i] ) )  ++result;
        }
        return result;
    }

    public void registerOption( String str ) {
        Vector v = (Vector) options.get( optionKind( str ) );
        if (v == null)  v = new Vector();
        v.addElement( optionValue( str ) );
        options.put( optionKind( str ), v );
    }

    private static String optionKind( String str ) {
        int e = str.indexOf( '=' );
        return (e == -1) ? str : str.substring( 0, e ).trim();
    }

    private static String optionValue( String str ) {
        int e = str.indexOf( '=' );
        return (e == -1) ? "" : str.substring( e + 1 ).trim();
    }

    public String getOption( String option_name ) {
        Vector v = (Vector) options.get( option_name );
        if (v == null || v.isEmpty())  return null;
        return (String) v.elementAt( 0 );
    }

    public String[] getOptions( String option_name ) {
        Vector v = (Vector) options.get( option_name );
        String[] result = new String[(v == null) ? 0 : v.size()];
        for (int i = 0; i < result.length; ++i) {
            result[i] = (String) v.elementAt( i );
        }
        return result;
    }

    public String getOption( String opt1, String opt2 ) {
        String result = getOption( opt1 );
        if (result == null)  result = getOption( opt2 );
        return result;
    }

    public int getDebugLevel() {
        int debug_level = 0;

        String level_str = getOption( "g" );
        if (level_str != null) {
            debug_level = Integer.valueOf( level_str ).intValue();
        }
        if (getOption( "verbose" ) != null)  debug_level |= DEBUG_VERBOSE;

        return debug_level;
    }

    public JavaCompiler getJavaCompiler()
        throws ClassNotFoundException, InstantiationException,
               IllegalAccessException
    {
        String compiler_name = getOption( "compiler", "c" );
        if (compiler_name == null) {
            compiler_name = "JP.ac.tsukuba.openjava.SunJavaCompiler";
        }
        Class clazz = Class.forName( compiler_name );
        JavaCompiler result = (JavaCompiler) clazz.newInstance();
        return result;
    }

    public boolean callerTranslation() {
        if (getOption( "calleroff" ) != null)  return false;
        return true;
    }

    public boolean qualifyNameFirst() {
        String qualifyname_flag = getOption( "callerfast" );
        if (qualifyname_flag == null)  qualifyname_flag = "true";
        if (qualifyname_flag.equals("false"))  return false;
        return true;
    }

    private void checkArguments() throws IOException {
        /* checks files */
        File[] files = this.getFiles();
        if (files.length == 0) {
            System.err.println( "Files are not specified." );
            throw new IOException();
        }
        for (int i = 0; i < files.length; ++i) {
            if (! files[i].canRead()) {
                System.err.println( "cannot find file " + files[i] );
                throw new IOException();
            }
            if (! files[i].getName().endsWith( ".oj" )) {
                System.err.println( "illegal file name " + files[i] );
                throw new IOException();
            }
        }
        String diststr = getOption( "d" );
        if (diststr != null && (! new File( diststr ).isDirectory())) {
            System.err.println( "Directory does not exist : " + diststr );
            throw new IOException();
        }
    }

}
