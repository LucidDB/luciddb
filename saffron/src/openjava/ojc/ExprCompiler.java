/*
 * ExprCompiler.java
 *
 *
 */
package openjava.ojc;


import java.io.*;
import java.util.Vector;
import java.util.Hashtable;
import java.util.Date;
import java.lang.reflect.Constructor;
import openjava.tools.parser.*;
import openjava.tools.DebugOut;
import openjava.mop.*;
import openjava.ptree.*;
import openjava.ptree.util.*;


public class ExprCompiler
{
    CommandArguments arguments;
    File files[];
    JavaCompiler java_compiler;
    CompilationUnit[] added_cu = null;

    FileEnvironment[] file_env;
    CompilationUnit[] comp_unit;

    ExprCompiler( CommandArguments arguments ) {
	super();
	this.arguments = arguments;
	this.files = arguments.getFiles();
	file_env = new FileEnvironment[files.length];
	comp_unit = new CompilationUnit[files.length];
	DebugOut.setDebugLevel( arguments.getDebugLevel() );
	try {
	    this.java_compiler = arguments.getJavaCompiler();
	    OJSystem.setJavaCompiler( this.java_compiler );
	} catch ( Exception e ) {
	    System.err.println( "illegal java compiler : " + e );
	}
    }

    public void run() {
	long total = 0;

	{
	    long begin, end;
	    System.err.println( "generateParseTree()" );
	    begin = System.currentTimeMillis();
	    generateParseTree( file_env, comp_unit );
	    end = System.currentTimeMillis();
	    total = calculateTime( total, end - begin );
	}

	initDebug();

	{
	    long begin, end;
	    System.err.println( "initParseTree()" );
	    begin = System.currentTimeMillis();
	    initParseTree( file_env, comp_unit );
	    end = System.currentTimeMillis();
	    total = calculateTime( total, end - begin );
	}

	{
	    long begin, end;
	    System.err.println( "translateCalleeSide()" );
	    begin = System.currentTimeMillis();
	    translateCalleeSide( file_env, comp_unit );
	    end = System.currentTimeMillis();
	    total = calculateTime( total, end - begin );
	}

	{
	    long begin, end;
	    System.err.println( "translateCallerSide()" );
	    begin = System.currentTimeMillis();
	    translateCallerSide( file_env, comp_unit );
	    end = System.currentTimeMillis();
	    total = calculateTime( total, end - begin );
	}

	{
	    long begin, end;
	    System.err.println( "generateAdditionalCompilationUnit()" );
	    begin = System.currentTimeMillis();
	    generateAdditionalCompilationUnit();
	    end = System.currentTimeMillis();
	    total = calculateTime( total, end - begin );
	}

	{
	    long begin, end;
	    System.err.println( "outputToFile()" );
	    begin = System.currentTimeMillis();
	    outputToFile( file_env, comp_unit );
	    end = System.currentTimeMillis();
	    total = calculateTime( total, end - begin );
	}

	{
	    long begin, end;
	    System.err.println( "javac()" );
	    begin = System.currentTimeMillis();
	    javac( file_env, comp_unit );
	    end = System.currentTimeMillis();
	    total = calculateTime( total, end - begin );
	}
    }

    static long calculateTime( long total, long time ) {
	total += time;
	System.err.println( "Time(ms): " + time + "\tTotal(ms): " + total );
	return total;
    }

    void generateParseTree( FileEnvironment[] file_env,
			    CompilationUnit[] comp_unit )
    {
        for (int i = 0; i < files.length; ++i) {
	    DebugOut.println( "parsing file " + files[i] );
	    try {
	        comp_unit[i] = parse( files[i] );
		file_env[i] = makeFileEnvironment( comp_unit[i] );
		String pubcls_sname = comp_unit[i].getPublicClass().getName();
		ClassDeclarationList typedecls
		    = comp_unit[i].getClassDeclarations();
		for (int j = 0; j < typedecls.size(); ++j) {
		    ClassDeclaration clazz_decl = typedecls.get( j );
		    OJClass c = makeOJClass( file_env[i], clazz_decl );
		    if (c.getSimpleName().equals( pubcls_sname ))  {
		        /* public class */
		      DebugOut.println( "main class " + c.getName() );
		      OJSystem.env.record( c.getName(), c );
		    } else {
		        /* non-public class */
		        DebugOut.println( "local class " + c.getName() );
			file_env[i].recordLocalClassName( c.getName() );
			OJSystem.env.record( c.getName(), c );
			/*file_env[i].record( c.getName(), c );*/
		    }
		    /*** should consider private **/
		    recordInnerClasses( c );
		}
	    } catch ( Exception e ) {
	        System.err.println( "errors during parsing. " + e );
	    }
	    DebugOut.println( "file environment : " );
	    DebugOut.println( file_env[i] );
        }

        DebugOut.println( "global environment : " );
	DebugOut.println( OJSystem.env );
    }

    private static void recordInnerClasses( OJClass c ) {
	OJClass[] inners = c.getDeclaredClasses();
	for (int i = 0; i < inners.length; ++i) {
	    OJSystem.env.record( inners[i].getName(), inners[i] );
	    recordInnerClasses( inners[i] );
	}
    }

    void generateAdditionalCompilationUnit() {
        OJClass[] added = OJSystem.addedClasses();
	this.added_cu = new CompilationUnit[added.length];
	for (int i = 0; i < added.length; ++i) {
	    ClassDeclarationList cdecls;
	    try {
	        cdecls = new ClassDeclarationList( added[i].getSourceCode() );
	    } catch ( Exception e ) {
		System.err.println( "errors during generating " + added[i] );
		e.printStackTrace();
		continue;
	    }
	    String pack = added[i].getPackage();
	    added_cu[i] = new CompilationUnit( pack, null, cdecls );
	}	    
    }

    private OJClass makeOJClass( Environment env, ClassDeclaration cdecl ) {
	OJClass result;
	String qname = env.toQualifiedName( cdecl.getName() );
	try {
	    Class meta = OJSystem.getMetabind( qname );
	    Constructor constr
		= meta.getConstructor( new Class[]{
		    Environment . class,
		    OJClass . class,
		    ClassDeclaration . class } );
	    result = (OJClass) constr.newInstance( new Object[]{
		env, null, cdecl } );
	} catch ( Exception e ) {
	    result = new OJClass( env, null, cdecl );
	}
	return result;
    }

    void initDebug() {
    }

    void outputToDebugFile( FileEnvironment[] fenv,
				    CompilationUnit[] comp_unit,
				    String suffix )
    {
	if (arguments.getDebugLevel() == 0)  return;
        for (int i = 0; i < comp_unit.length; ++i) {
	    File outfile = null;
	    try {
	        outfile = getOutputFile( files[i], comp_unit[i], suffix );
		FileWriter fout = new FileWriter( outfile );
		PrintWriter out = new PrintWriter( fout );
		SourceCodeWriter writer = new SourceCodeWriter( out );
		writer.setDebugLevel( arguments.getDebugLevel() );
		comp_unit[i].accept( writer );
		out.flush();  out.close();
	    } catch ( Exception e ) {
		System.err.println( "errors during printing " + outfile );
		e.printStackTrace();
	    }
	}
    }

    void outputToFile( FileEnvironment[] fenv,
			       CompilationUnit[] comp_unit )
    {
        for (int i = 0; i < comp_unit.length; ++i) {
	    File outfile = null;
	    try {
	        outfile = getOutputFile( files[i], comp_unit[i], ".java" );
		FileWriter fout = new FileWriter( outfile );
		PrintWriter out = new PrintWriter( fout );
		SourceCodeWriter writer = new SourceCodeWriter( out );
		writer.setDebugLevel( 0 );
		comp_unit[i].accept( writer );
		out.flush();  out.close();
	    } catch ( Exception e ) {
		System.err.println( "errors during printing " + outfile );
		e.printStackTrace();
	    }

	    try {
	        outfile = getOutputFile( files[i], comp_unit[i],
					      MetaInfo.SUFFIX + ".java" );
		String qname = fenv[i].toQualifiedName( baseName( files[i] ) );
		OJClass clazz = OJClass.forName( qname );
		FileWriter fout = new FileWriter( outfile );
		clazz.writeMetaInfo( fout );
		fout.flush();  fout.close();
	    } catch ( Exception e ) {
		System.err.println( "errors during printing " + outfile );
		e.printStackTrace();
	    }
        }

	for (int i = 0; i < added_cu.length; ++i) {
	    File outfile = null;
	    try {
	        outfile = getOutputFile( null, added_cu[i], ".java" );
		FileWriter fout = new FileWriter( outfile );
		PrintWriter out = new PrintWriter( fout );
		SourceCodeWriter writer = new SourceCodeWriter( out );
		writer.setDebugLevel( 0 );
		added_cu[i].accept( writer );
		out.flush();  out.close();
	    } catch ( Exception e ) {
		System.err.println( "errors during printing " + outfile );
		e.printStackTrace();
	    }
	}	    
    }

    private static final String class2path( String cname ) {
        return cname.replace( '.', File.separatorChar );
    }
       
    private File getOutputFile( File fin, CompilationUnit comp_unit,
				String suffix )
        throws ParseTreeException
    {
        String pack = comp_unit.getPackage();
	String name = comp_unit.getPublicClass().getName();
        return getOutputFile( fin, pack, name, suffix );
    }

    private File getOutputFile( File fin, String pack, String name,
				String suffix )
    {
        File dir; /* directory to put the file in */
        String distbase = arguments.getOption( "d" );
	if (distbase == null && fin != null) {
	    /* the same directory where source file is */
	    String dirname = fin.getParent();
	    if (dirname == null)  dirname = ".";
	    dir = new File( dirname );
	} else {
	    if (distbase == null)  distbase = ".";
	    File basedir = new File( distbase );
	    if (pack == null || pack.equals( "" )) {
	        dir = basedir;
	    } else {
	        dir = new File( basedir, class2path( pack ) );
		if ((! dir.isDirectory()) && (! dir.mkdirs()) ) {
		    System.err.println( "fail to create dir : " + dir );
		    dir = new File( "." );
		}
	    }
	}
	return new File( dir, name + suffix );
    }

    private static String baseName( File file ) {
	String name = file.getName();
	int index = name.lastIndexOf( '.' );
	if (index == -1)  return name;
	return name.substring( 0, index );
    }

    void initParseTree( FileEnvironment[] file_env,
				      CompilationUnit[] comp_unit )
    {
        for (int i = 0; i < comp_unit.length; ++i) {
	    if (comp_unit[i] == null) {
		System.err.println( files[i] + " is skipped." );
		continue;
	    }
	    try {
		//comp_unit[i].accept( new TypeNameQualifier( file_env[i] ) );
		MemberAccessCorrector corrector
		    = new MemberAccessCorrector( file_env[i] );
		comp_unit[i].accept( corrector );
	    } catch ( ParseTreeException e ) {
		System.err.println( "Encountered errors during analysis." );
		e.printStackTrace();
	    }
        }
    }

    void translateCalleeSide( FileEnvironment[] file_env,
				      CompilationUnit[] comp_unit )
    {
        Hashtable under_c = OJSystem.underConstruction;
        for (int i = 0; i < comp_unit.length; ++i) {
	    if (comp_unit[i] == null) {
		System.err.println( files[i] + " is skipped." );
		continue;
	    }
	    ClassDeclarationList cdecls = comp_unit[i].getClassDeclarations();
	    for (int j = 0; j < cdecls.size(); ++j) {
		ClassDeclaration cdecl = cdecls.get( j );
		String qname = file_env[i].toQualifiedName( cdecl.getName() );
		OJClass clazz;
		try {
		    clazz = OJClass.forName( qname );
		} catch ( OJClassNotFoundException e ) {
		    System.err.println( "no " + qname + " : " + e );
		    continue;
		}
		under_c.put( clazz,
			     new TranslatorThread( file_env[i], clazz ) );
	    }
        }

	try {
	    while (! under_c.isEmpty()) {
	        OJClass clazz = (OJClass) under_c.keys().nextElement();
		resolveOrder( clazz );
	    }
	} catch ( InterruptedException e ) {
	    System.err.println( "translation failed : " + e );
	}
    }

    private void resolveOrder( OJClass clazz )
        throws InterruptedException
    {
        /* lock for the given class metaobject */
        Object lock = new Object();
        Hashtable under_c = OJSystem.underConstruction;

	synchronized (clazz) {
	    OJSystem.orderingLock = lock;
	    ((TranslatorThread) under_c.get( clazz )).start();
	    clazz.wait();
	}
	
        while (OJSystem.waited != null) {
	    resolveOrder( OJSystem.waited );

	    synchronized (clazz) {
	        OJSystem.orderingLock = lock;
	        synchronized (lock) {
		    lock.notifyAll();
		}
		clazz.wait();
	    }
	}

	under_c.remove( clazz );
    }

    void translateCallerSide( FileEnvironment[] file_env,
				      CompilationUnit[] comp_unit )
    {
        for (int i = 0; i < comp_unit.length; ++i) {
	    if (comp_unit[i] == null) {
		System.err.println( files[i] + " is skipped." );
		continue;
	    }
	    try {
		ExpansionApplier expander
		    = new ExpansionApplier( file_env[i] );
		comp_unit[i].accept( expander );
	    } catch ( ParseTreeException e ) {
		System.err.println( "Encountered errors during printing." );
		e.printStackTrace();
	    }
        }
    }

    private static CompilationUnit parse( File file ) {
        Parser parser;

        try {
            parser = new Parser( new java.io.FileInputStream( file ) );
        } catch ( java.io.FileNotFoundException e ) {
            System.err.println( "File " + file + " not found." );
            return null;
        }

        CompilationUnit result;
        try {
            result = parser.CompilationUnit( OJSystem.env );
        } catch (ParseException e) {
            System.err.println( "Encountered errors during parse." );
            e.printStackTrace();
            result = null;
        }

        return result;
    }

    private static FileEnvironment
    makeFileEnvironment( CompilationUnit comp_unit )
        throws ParseTreeException
    {
        String pack = comp_unit.getPackage();
	String class_sname = comp_unit.getPublicClass().getName();
        FileEnvironment result
	    = new FileEnvironment( OJSystem.env, pack, class_sname );

        String[] imps = comp_unit.getDeclaredImports();
	for (int i = 0; i < imps.length; ++i) {
            if (CompilationUnit.isOnDemandImport(imps[i] )) {
                String imppack = CompilationUnit.trimOnDemand( imps[i] );
                result.importPackage( imppack );
            } else {
                result.importClass( imps[i] );
            }
	}

        return result;
    }

    /**
     * Compiles the generated files into byte codes
     */
    private void javac( FileEnvironment[] fenv, CompilationUnit[] comp_unit ) {
        String[] options = arguments.getOptions( "C" );
	String[] srcs = new String[files.length * 2 + added_cu.length];
	try {
	    for (int i = 0; i < files.length; ++i) {
	        File javafile;
		javafile = getOutputFile( files[i], comp_unit[i], ".java" );
		srcs[i * 2] = javafile.getPath();
		javafile = getOutputFile( files[i], comp_unit[i],
					  MetaInfo.SUFFIX + ".java" );
		srcs[i * 2 + 1] = javafile.getPath();
	    }
	    for (int i = 0; i < added_cu.length; ++i) {
	        File javafile
		    = getOutputFile( files[i], added_cu[i], ".java" );
		srcs[files.length * 2 + i] = javafile.getPath();
	    }
	    String[] args = new String[options.length + srcs.length];
	    System.arraycopy( options, 0, args, 0, options.length );
	    System.arraycopy( srcs, 0, args, options.length, srcs.length );

	    /*sun.tools.javac.Main.main( args );*/
		java_compiler.getArgs().setStringArray(args);
	    java_compiler.compile();
	} catch ( Exception e ) {
	    System.err.println( "errors during compiling into bytecode." );
	    e.printStackTrace();
	}
    }

}
