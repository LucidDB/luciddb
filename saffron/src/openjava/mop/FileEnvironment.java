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


import java.io.StringWriter;
import java.io.PrintWriter;
import java.util.Vector;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.HashSet;
import openjava.tools.DebugOut;
import openjava.ptree.*;


/**
 * An environment based upon a Java source file.
 *
 * <p>The symbols available are those specified in <code>import</code>
 * statements, and the class(es) defined in this file.
 *
 * @author   Michiaki Tatsubori
 * @version  1.0
 * @since    %SOFTWARE% 1.0
 * @see java.lang.Object
 */
public class FileEnvironment extends Environment
{
    private String packageName;
    private String mainClassName;
    private Vector localClasses = new Vector(); /*not implemented*/

    private Vector importedClasses = new Vector();
    private Vector importedPackages = new Vector();
    private Hashtable localClassTable = new Hashtable();

    public FileEnvironment( Environment e, String pack, String name ) {
        super( e );
        if (pack != null && pack.equals( "" ))  pack = null;
        this.packageName = pack;
        this.mainClassName = toSimpleName( name );
        importPackage( "java.lang" );
    }

    public FileEnvironment( Environment e ) {
        super( e );
        this.packageName = null;
        this.mainClassName = null;
        importPackage( "java.lang" );
    }

    public FileEnvironment(Environment e, CompilationUnit comp_unit,
                           String mainname) {
        this(e, comp_unit.getPackage(), mainname);

        String[] imps = comp_unit.getDeclaredImports();
        for (int i = 0; i < imps.length; ++i) {
            if (CompilationUnit.isOnDemandImport(imps[i] )) {
                String imppack = CompilationUnit.trimOnDemand(imps[i]);
                this.importPackage( imppack );
            } else {
                this.importClass( imps[i] );
            }
        }

        ClassDeclarationList classdecls = comp_unit.getClassDeclarations();
        for (int i = 0; i < classdecls.size(); ++i) {
            ClassDeclaration cdecl = classdecls.get(i);
            if (cdecl.getName().equals(mainname))  continue;
            this.recordLocalClassName(cdecl.getName());
        }
    }

    public String toString() {
        StringWriter str_writer = new StringWriter();
        PrintWriter out = new PrintWriter( str_writer );

        out.println( "FileEnvironment" );
        out.println( "\tpackage : " + packageName );
        out.println( "\tmain class : " + mainClassName );
        out.print( "\tlocal classes : " );
        writeStringVector( out, localClasses );
        out.println();
        out.print( "\timported classes : " );
        writeStringVector( out, importedClasses );
        out.println();
        out.print( "\timported packages : " );
        writeStringVector( out, importedPackages );
        out.println();
        out.println( "\tlocal class table : " + localClassTable );
        out.println( "parent env : " + parent );

        out.flush();
        return str_writer.toString();
    }

    private static void writeStringVector( PrintWriter out, Vector v ) {
        Enumeration e = v.elements();
        while (e.hasMoreElements()) {
            String str = (String) e.nextElement();
            out.print( str + " " );
        }
    }

    public String getPublicClassName() {
        return mainClassName;
    }

    /**
     * Sets the package name.
     */
    public void setPackage( String pack ) {
        if (pack != null && pack.equals( "" ))  pack = null;
        packageName = pack;
    }

    /**
     * Gets the package name.
     */
    public String getPackage() {
        if (packageName == "")  return null;
        return packageName;
    }

    /**
     * Looks a class object up.
     *
     * @param   name            the name of the fully-qualified name of
     *                          the class looked for
     */
    public OJClass lookupClass( String name ) {
        OJClass clazz = (OJClass) localClassTable.get( name );
        if (clazz != null)  return clazz;
        try {
            clazz = OJClass.forName( name );
        } catch ( OJClassNotFoundException e ) {
            return null;
        }
        return clazz;
    }

    /**
     * Records a class object.
     *
     * @param   name            the fully-qualified name of the class
     * @param   clazz           the class object associated with that name
     */
    public void record( String name, OJClass clazz ) {
        DebugOut.println( "Fenv#record(): " + name + " " + clazz );
        localClassTable.put( name, clazz );
    }

    /**
     * Imports a class.
     *
     * @param qualified_name fully-qualified name of the imported class
     * @return  false if the given name is already registered or
     *          if it is an ambiguous name.
     */
    public boolean importClass( String qualified_name ) {
        DebugOut.println( "FileEnvironment#importClass() : "
                         + qualified_name );
        String simple_name = toSimpleName( qualified_name );
        if (isAlreadyImportedClass( qualified_name )
            || isCrashingClassName( simple_name )) {
            return false;
        } else {
            importedClasses.addElement(qualified_name);
            return true;
        }
    }

    /**
     * Imports a package.
     *
     * @param name      the fully-qualified name of the imported package
     */
    public void importPackage( String name ) {
        DebugOut.println( "FileEnvironment#importPackage() : " + name );
        importedPackages.addElement( name );
    }

    /**
     * Obtains the fully-qualified name of the given class name.
     *
     * @param  name  a simple class name or a fully-qualified class name
     * @return  the fully-qualified name of the class
     */
    public String toQualifiedName( String name ) {
        if (name == null || isPrimitiveType( name )
            || isQualifiedName( name )) {
            return name;
        }

        if (name.endsWith("[]")) {
            String stripped = name.substring( 0, name.length() - 2 );
            return toQualifiedName( stripped ) + "[]";
        }

        String sname = name;

        if (sname.equals( mainClassName )
            || localClasses.indexOf( sname ) != -1)
        {
            String pack = getPackage();
            if (pack == null)  return sname;
            return pack + "." + sname;
        }

        String qname;

        qname = searchImportedClasses( sname );
        if(qname != null)  return qname;

        qname = searchImportedPackages( sname );
        if(qname != null)  return qname;

        return sname;
    }

    /**
     * Register a simple name as a local class
     */
    public void recordLocalClassName( String name ) {
        DebugOut.println( "FileEnvironment#recordLocalClassName() : "
                           + toSimpleName( name ) );
        localClasses.addElement( toSimpleName( name ) );
    }

    private static final boolean isPrimitiveType( String name ) {
        if (name == null)  return false;
        return (name.equals("boolean")
                || name.equals("byte")
                || name.equals("char")
                || name.equals("double")
                || name.equals("float")
                || name.equals("int")
                || name.equals("long")
                || name.equals("short")
                || name.equals("void"));
    }

    private String searchImportedClasses( String simple_name ) {
        for (int i = 0, size = importedClasses.size(); i < size; ++i) {
            String qname = (String)importedClasses.elementAt(i);
            if (qname.endsWith(simple_name))  return qname;
        }
        return null;
    }

    private boolean isAlreadyImportedClass( String qualified_name ) {
        for (int i = 0, size = importedClasses.size(); i < size; ++i) {
            String qname = (String)importedClasses.elementAt(i);
            if (qname.equals( qualified_name )) return true;
        }
        return false;
    }

    private boolean isCrashingClassName( String simple_name ) {
        return (searchImportedClasses( simple_name ) != null);
    }

    private String searchImportedPackages( String simple_name ) {
        String found;

        String packname = getPackage();
        if (packname == null || packname.equals( "" )) {
            found = simple_name;
        } else {
            found = packname + "." + simple_name;
        }
        if (theClassExists( found ))  return found;

        found = null;

        /* the qualified name found last should be taken */
        for (int i = 0, size = importedPackages.size(); i < size; ++i) {
            packname = (String) importedPackages.elementAt( i );
            String qname = packname + "." + simple_name;
            if (theClassExists( qname ))  found = qname;
        }

        /* this returns the qualified name found last. */
        return found;
    }

    /*
     * The use of Class.forName() costs too much time.
     */
    private boolean theClassExists( String class_name ) {
        synchronized (nonExistentClasses) {
            if (nonExistentClasses.contains(class_name)) {
                return false;
            }
            OJClass clazz = lookupClass(class_name);
            if (clazz == null) {
                nonExistentClasses.add(class_name);
            }
            return clazz != null;
        }
    }

    /**
     * Cache to speed up {@link #theClassExists}.
     */
    private static HashSet nonExistentClasses = new HashSet();

    /**
     * binds a name to the class type.
     *
     * @param     name            the fully-qualified name of the class
     * @param     info           the class object associated with that name
     */
    public void bindVariable( String name, VariableInfo info ) {
        System.err.println( "error : illegal binding on FileEnvironment" );
    }

    public VariableInfo lookupBind( String name ) {
        return null;
    }

}


