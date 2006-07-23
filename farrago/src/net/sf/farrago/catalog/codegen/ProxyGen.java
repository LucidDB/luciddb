/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.catalog.codegen;

import java.io.*;

import java.lang.reflect.*;

import java.util.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import net.sf.farrago.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.util.*;

import org.eigenbase.util.*;


/**
 * ProxyGen generates read-only C++ JNI proxies for MDR-generated Java
 * interfaces (something like a stripped-down JACE). It uses an unholy mix of
 * Java/JMI navel-scrutiny. For an example of its output, see
 * FemGeneratedClasses.h and FemGeneratedMethods.h in //open/fennel/farrago.
 *
 * <p>To understand this generator, it's important to distinguish among
 * MOF/UML/JMI classes (which are metadata objects), repository-generated Java
 * interfaces, and the C++ proxy classes generated here.</p>
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ProxyGen
{

    //~ Static fields/initializers ---------------------------------------------

    private static final Comparator classNameComparator =
        new Comparator() {
            public int compare(
                Object o1,
                Object o2)
            {
                final String name1 = ((Class) o1).getName();
                final String name2 = ((Class) o2).getName();
                return name1.compareTo(name2);
            }
        };


    //~ Instance fields --------------------------------------------------------

    /**
     * Map from Class to corresponding C++ type name as String.
     */
    private Map cppTypeMap = new HashMap();

    /**
     * Map from Class to RefClass for everything in genInterfaces.
     */
    private Map javaToJmiMap = new HashMap();

    /**
     * Map from Class to corresponding Java type String to use in method
     * signatures.
     */
    private Map javaTypeMap = new HashMap();

    /**
     * PrintWriter used to generate output.
     */
    private PrintWriter pw;

    /**
     * Set containing all interfaces (represented as Class objects) for which
     * C++ proxies are to be generated.
     */
    private Set genInterfaces = new HashSet();

    /**
     * Set containing all base interfaces (represented as Class objects) from
     * which C++ proxies are to inherit.
     */
    private Set baseInterfaces = new HashSet();

    /**
     * Set containing interfaces (represented as Class objects) whose proxy
     * definition has not yet been generated. This is used to induce topological
     * order for the inheritance graph.
     */
    private Set undefinedInterfaces = new HashSet();

    /**
     * Set containing all interfaces (represented as Class objects) for which
     * C++ enums are to be generated.
     */
    private Set genEnums = new HashSet();
    private String genPrefix;
    private String basePrefix;
    private String visitorClassName;
    private String visitorBaseName;

    //~ Constructors -----------------------------------------------------------

    /**
     * Initialize a new ProxyGen.
     */
    public ProxyGen(
        String genPrefix,
        String basePrefix)
    {
        this.genPrefix = genPrefix;
        this.basePrefix = basePrefix;

        visitorClassName = genPrefix + "Visitor";

        if (basePrefix == null) {
            visitorBaseName = "JniProxyVisitor";
        } else {
            visitorBaseName = basePrefix + "Visitor";
        }

        cppTypeMap.put(Integer.TYPE, "int32_t");
        javaTypeMap.put(Integer.TYPE, "I");

        cppTypeMap.put(Long.TYPE, "int64_t");
        javaTypeMap.put(Long.TYPE, "J");

        cppTypeMap.put(Short.TYPE, "int16_t");
        javaTypeMap.put(Short.TYPE, "S");

        cppTypeMap.put(Double.TYPE, "double");
        javaTypeMap.put(Double.TYPE, "D");

        cppTypeMap.put(Float.TYPE, "float");
        javaTypeMap.put(Float.TYPE, "F");

        cppTypeMap.put(Boolean.TYPE, "bool");
        javaTypeMap.put(Boolean.TYPE, "Z");

        cppTypeMap.put(String.class, "std::string");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Adds all classes from a JMI package to the set of interfaces to be
     * generated.
     *
     * @param refPackage the source JMI package
     */
    public void addGenClasses(RefPackage refPackage)
        throws ClassNotFoundException
    {
        Iterator iter = refPackage.refAllClasses().iterator();
        while (iter.hasNext()) {
            RefClass refClass = (RefClass) iter.next();
            Class clazz = JmiUtil.getJavaInterfaceForRefObject(refClass);
            genInterfaces.add(clazz);
            javaToJmiMap.put(clazz, refClass);
        }
    }

    /**
     * Adds all classes from a JMI package to the set of interfaces to use as
     * bases.
     *
     * @param refPackage the source JMI package
     */
    public void addBaseClasses(RefPackage refPackage)
        throws ClassNotFoundException
    {
        Iterator iter = refPackage.refAllClasses().iterator();
        while (iter.hasNext()) {
            RefClass refClass = (RefClass) iter.next();
            Class clazz = JmiUtil.getJavaInterfaceForRefObject(refClass);
            baseInterfaces.add(clazz);
            javaToJmiMap.put(clazz, refClass);
        }
    }

    /**
     * Generates the C++ class definition for one interface.
     *
     * @param clazz the interface
     */
    public void generateClassDefinition(Class clazz)
    {
        assert (clazz.isInterface());
        if (!undefinedInterfaces.contains(clazz)) {
            // this class already done
            return;
        }
        Class [] bases = clazz.getInterfaces();

        // make sure bases are already defined
        for (int i = 0; i < bases.length; ++i) {
            if (bases[i] == RefObject.class) {
                continue;
            }
            generateClassDefinition(bases[i]);
        }
        undefinedInterfaces.remove(clazz);
        pw.println("class " + getCppClassName(clazz));
        pw.print(": virtual public JniProxy");
        for (int i = 0; i < bases.length; ++i) {
            if (bases[i] == RefObject.class) {
                continue;
            }
            pw.print(", virtual public ");
            pw.print(getCppClassName(bases[i]));
        }
        pw.println();
        pw.println("{");
        pw.println("public:");
        Method [] methods = clazz.getDeclaredMethods();
        for (int i = 0; i < methods.length; ++i) {
            if (isGetter(methods[i])) {
                generateMethodDeclaration(methods[i]);
            }
        }
        pw.println("};");
        pw.println();
    }

    /**
     * Generates the C++ code for all class definitions.
     *
     * @param pw output
     */
    public void generateClassDefinitions(PrintWriter pw)
    {
        this.pw = pw;

        pw.println("// This code generated by ProxyGen -- do not edit");
        pw.println();

        // First, generate forward references for all classes we're going to
        // generate.  This makes possible any kind of class dependency graph,
        // since objects are always returned by reference.
        Class [] interfaces = toSortedArray(genInterfaces);
        for (int i = 0; i < interfaces.length; i++) {
            Class clazz = interfaces[i];
            generateClassDeclaration(clazz);
            pw.println();
        }

        // Next, generate the actual class definitions.  We need to generate
        // base classes before derived classes, so use undefinedInterfaces to
        // keep track.
        undefinedInterfaces.clear();
        undefinedInterfaces.addAll(genInterfaces);

        for (int i = 0; i < interfaces.length; i++) {
            Class clazz = interfaces[i];
            generateClassDefinition(clazz);
        }

        // Finally, generate the visitor interface with a visitor method
        // overload for each class.
        pw.println(
            "class " + visitorClassName + " : virtual public "
            + visitorBaseName);
        pw.println("{");
        pw.println("public:");
        pw.println(
            "static JniProxyVisitTable<" + visitorClassName
            + "> visitTbl;");
        for (int i = 0; i < interfaces.length; i++) {
            Class clazz = interfaces[i];
            generateVisitDeclaration(clazz);
        }
        pw.println("};");
        pw.println();
    }

    /**
     * Generates the C++ code for all enumerations.
     *
     * @param pw output
     */
    public void generateEnumDefinitions(PrintWriter pw)
        throws Exception
    {
        pw.println("// This code generated by ProxyGen -- do not edit");
        pw.println();

        CppEnumGen enumGen = new CppEnumGen(pw);

        final Class [] enumInterfaces = toSortedArray(genEnums);
        for (int i = 0; i < enumInterfaces.length; i++) {
            Class enumInterface = enumInterfaces[i];
            Class enumClass = Class.forName(enumInterface.getName() + "Enum");
            enumGen.generateEnumForClass(
                ReflectUtil.getUnqualifiedClassName(enumInterface),
                enumClass,
                enumClass);
        }
    }

    /**
     * Generates the C++ code for all method definitions.
     *
     * @param pw output
     */
    public void generateClassImplementations(PrintWriter pw)
    {
        this.pw = pw;

        pw.println("// This code generated by ProxyGen -- do not edit");
        pw.println();

        // First, generate static definitions such as all method ID's used by
        // generated getters.
        pw.println(
            "JniProxyVisitTable<" + visitorClassName + "> "
            + visitorClassName + "::visitTbl;");
        pw.println();
        Class [] interfaces = toSortedArray(genInterfaces);
        for (int i = 0; i < interfaces.length; i++) {
            Class clazz = interfaces[i];
            generateStaticDefinitions(clazz);
        }

        // Next, generate the static method which performs the JNI lookup for
        // methods.
        pw.println(
            "void staticInit" + genPrefix
            + "(JniEnvRef pEnv,JniProxyVisitTableBase &visitTbl)");
        pw.println("{");
        pw.println("jclass jClass;");
        for (int i = 0; i < interfaces.length; i++) {
            Class clazz = interfaces[i];
            generateStaticInitialization(clazz);
        }
        pw.println("}");
        pw.println();

        // Finally, generate the getter implementations for each method.
        for (int i = 0; i < interfaces.length; i++) {
            Class clazz = interfaces[i];
            Method [] methods = clazz.getDeclaredMethods();
            for (int j = 0; j < methods.length; ++j) {
                if (isGetter(methods[j])) {
                    generateMethodDefinition(methods[j]);
                    pw.println();
                }
            }
        }
    }

    /**
     * Converts collection into an array of classes sorted by name. This is
     * necessary in order to make the output deterministic.
     */
    private static Class [] toSortedArray(Collection collection)
    {
        Class [] classes =
            (Class []) collection.toArray(new Class[collection.size()]);
        Arrays.sort(classes, classNameComparator);
        return classes;
    }

    /**
     * Generates the C++ declaration for one method.
     *
     * @param method .
     */
    public void generateMethodDeclaration(Method method)
    {
        String cppTypeName = getCppReturnTypeName(method);
        pw.print(cppTypeName);
        pw.print(" ");
        pw.print(method.getName());
        pw.println("();");

        // TODO:  make private?
        pw.print("static jmethodID meth_");
        pw.print(method.getName());
        pw.println(";");
    }

    /**
     * Generates the C++ definition for one method
     *
     * @param method .
     */
    public void generateMethodDefinition(Method method)
    {
        String cppTypeName = getCppReturnTypeName(method);
        pw.print(cppTypeName);
        pw.print(" ");
        pw.print(getCppClassName(method.getDeclaringClass()));
        pw.print("::");
        pw.print(method.getName());
        pw.println("()");
        pw.println("{");
        Class returnType = method.getReturnType();
        if (returnType.isPrimitive()) {
            pw.print("return pEnv->Call");
            pw.print(Character.toUpperCase(returnType.getName().charAt(0)));
            pw.print(returnType.getName().substring(1));
            pw.print("Method(jObject,meth_");
            pw.print(method.getName());
            pw.println(");");
        } else if (returnType.equals(String.class)) {
            pw.print("return constructString(");
            pw.print("pEnv->CallObjectMethod(jObject,meth_");
            pw.print(method.getName());
            pw.println("));");
        } else if (RefEnum.class.isAssignableFrom(returnType)) {
            // TODO jvs 29-April-2004:  Need to find a way to filter out
            // enumerations from base packages, otherwise we'll generate
            // duplicates.
            genEnums.add(returnType);
            pw.print("std::string symbol = constructString(");
            pw.print("JniUtil::toString(pEnv,");
            pw.print("pEnv->CallObjectMethod(jObject,meth_");
            pw.print(method.getName());
            pw.println(")));");
            String enumName = ReflectUtil.getUnqualifiedClassName(returnType);
            pw.print("return static_cast<");
            pw.print(enumName);
            pw.print(">(JniUtil::lookUpEnum(");
            pw.print(enumName);
            pw.println("_names,symbol));");
        } else {
            pw.print(cppTypeName);
            pw.println(" p;");
            pw.println("p->pEnv = pEnv;");
            pw.print("p->jObject = pEnv->CallObjectMethod(jObject,meth_");
            pw.print(method.getName());
            pw.println(");");
            if (Collection.class.isAssignableFrom(returnType)) {
                pw.println("p.jIter = JniUtil::getIter(p->pEnv,p->jObject);");
                pw.println("++p;");
            } else {
                pw.println("if (!p->jObject) p.reset();");
            }
            pw.println("return p;");
        }
        pw.println("}");
    }

    /**
     * Main generator entry point invoked by build.xml (target
     * "generateFemCpp"). The catalog must already exist before running the
     * generator.
     *
     * @param args
     *
     * <ul>
     * <li>args[0] = filename for C++ class definition output
     * <li>args[1] = filename for C++ class implementation output
     * <li>args[2] = filename for C++ enumeration output
     * <li>args[3] = qualified name of source model package
     * <li>args[4] = prefix to use for generated objects
     * <li>args[5] = (optional) qualified name of model package to reference
     * as base; if this is not specified, generated objects are base classes
     * <li>args[6] = (optional) prefix to reference for base classes; must be
     * specified together with previous argument
     * </ul>
     */
    public static void main(String [] args)
        throws Exception
    {
        assert (args.length > 3);
        FileWriter defWriter = new FileWriter(args[0]);
        FileWriter implWriter = new FileWriter(args[1]);
        FileWriter enumWriter = new FileWriter(args[2]);
        String sourcePackageName = args[3];
        String genPrefix = args[4];

        String basePackageName = null;
        String basePrefix = null;
        if (args.length > 5) {
            assert (args.length == 7);
            basePackageName = args[5];
            basePrefix = args[6];
        }

        FarragoModelLoader modelLoader = null;
        ProxyGen proxyGen = new ProxyGen(genPrefix, basePrefix);
        try {
            modelLoader = new FarragoModelLoader();
            FarragoPackage farragoPackage =
                modelLoader.loadModel("FarragoCatalog", false);
            proxyGen.addGenClasses(
                findPackage(farragoPackage, sourcePackageName));

            if (basePackageName != null) {
                proxyGen.addBaseClasses(
                    findPackage(farragoPackage, basePackageName));
            }

            PrintWriter pw = new PrintWriter(defWriter);
            proxyGen.generateClassDefinitions(pw);
            pw.flush();

            pw = new PrintWriter(implWriter);
            proxyGen.generateClassImplementations(pw);
            pw.flush();

            pw = new PrintWriter(enumWriter);
            proxyGen.generateEnumDefinitions(pw);
            pw.flush();
        } finally {
            defWriter.close();
            implWriter.close();
            enumWriter.close();
            if (modelLoader != null) {
                modelLoader.close();
            }
        }
    }

    private static RefPackage findPackage(
        RefPackage rootPackage,
        String qualifiedName)
    {
        String [] sourcePackageNames = qualifiedName.split("\\.");
        RefPackage sourcePackage =
            JmiUtil.getSubPackage(rootPackage,
                sourcePackageNames,
                sourcePackageNames.length);
        assert (sourcePackage != null);
        return sourcePackage;
    }

    /**
     * Gets the name of the C++ type for a proxy instance.
     *
     * @param clazz the source Java interface
     *
     * @return corresponding C++ type name
     */
    private String getCppClassName(Class clazz)
    {
        String prefix;

        if (baseInterfaces.contains(clazz)) {
            prefix = basePrefix;
        } else {
            prefix = genPrefix;
        }

        return
            ReflectUtil.getUnqualifiedClassName(clazz).replaceFirst(prefix,
                "Proxy");
    }

    /**
     * Gets the name of the C++ type used to return a proxy instance by
     * reference.
     *
     * @param clazz the source Java interface
     *
     * @return corresponding C++ type name
     */
    private String getCppRefName(Class clazz)
    {
        return "Shared" + getCppClassName(clazz);
    }

    private String getCppReturnTypeName(Method method)
    {
        Class returnType = method.getReturnType();
        if (Collection.class.isAssignableFrom(returnType)) {
            // strip off "get"
            String attrName = method.getName().substring(3);

            // determine the name of the class on the other end of the
            // association
            RefClass refClass = toJmiClass(method.getDeclaringClass());
            MofClass mofClass = (MofClass) refClass.refMetaObject();
            Iterator iter = mofClass.getContents().iterator();
            while (iter.hasNext()) {
                Object obj = iter.next();
                if (obj instanceof Reference) {
                    Reference reference = (Reference) obj;
                    String endName = reference.getReferencedEnd().getName();
                    if (endName.equalsIgnoreCase(attrName)) {
                        return
                            "SharedProxy"
                            + reference.getReferencedEnd().getType().getName();
                    }
                }
                if (obj instanceof Attribute) {
                    Attribute attribute = (Attribute) obj;
                    if (attribute.getName().equalsIgnoreCase(attrName)) {
                        return "SharedProxy" + attribute.getType().getName();
                    }
                }
            }
        } else if (RefEnum.class.isAssignableFrom(returnType)) {
            return ReflectUtil.getUnqualifiedClassName(returnType);
        }
        String cppTypeName = (String) cppTypeMap.get(returnType);
        if (cppTypeName != null) {
            return cppTypeName;
        }
        return getCppRefName(returnType);
    }

    /**
     * Decides whether a Java method is a getter for a JMI attribute. We ignore
     * all others.
     *
     * @param method the Java method
     *
     * @return true iff we consider it a getter
     */
    private boolean isGetter(Method method)
    {
        String methodName = method.getName();
        return
            (methodName.startsWith("get") || methodName.startsWith("is"))
            && (method.getParameterTypes().length == 0);
    }

    private String getJavaTypeSignature(Class clazz)
    {
        String s = (String) javaTypeMap.get(clazz);
        if (s != null) {
            return s;
        }
        return "L" + clazz.getName().replace('.', '/') + ";";
    }

    private void generateClassDeclaration(Class clazz)
    {
        pw.print("class ");
        pw.print(getCppClassName(clazz));
        pw.println(";");

        pw.print("typedef JniProxyIter<");
        pw.print(getCppClassName(clazz));
        pw.print("> ");
        pw.print(getCppRefName(clazz));
        pw.println(";");
    }

    private void generateStaticDefinitions(Class clazz)
    {
        Method [] methods = clazz.getDeclaredMethods();
        for (int i = 0; i < methods.length; ++i) {
            if (isGetter(methods[i])) {
                pw.print("jmethodID ");
                pw.print(getCppClassName(clazz));
                pw.print("::meth_");
                pw.print(methods[i].getName());
                pw.println(" = 0;");
            }
        }
    }

    private void generateStaticInitialization(Class clazz)
    {
        pw.print("jClass = pEnv->FindClass(\"");
        pw.print(clazz.getName().replace('.', '/'));
        pw.println("\");");
        pw.print("visitTbl.addMethod(");
        pw.print("jClass,");
        pw.print(
            "JniProxyVisitTable<" + visitorClassName
            + ">::SharedVisitorMethod(");
        pw.print(
            "new JniProxyVisitTable<" + visitorClassName
            + ">::VisitorMethodImpl<");
        pw.print(getCppClassName(clazz));
        pw.println(">));");
        Method [] methods = clazz.getDeclaredMethods();
        for (int i = 0; i < methods.length; ++i) {
            if (isGetter(methods[i])) {
                pw.print(getCppClassName(clazz));
                pw.print("::meth_");
                pw.print(methods[i].getName());
                pw.print(" = pEnv->GetMethodID(jClass,\"");
                pw.print(methods[i].getName());
                pw.print("\",\"()");
                pw.print(getJavaTypeSignature(methods[i].getReturnType()));
                pw.println("\");");
            }
        }
        pw.println();
    }

    private void generateVisitDeclaration(Class clazz)
    {
        // Visitor methods are defined to call a generic notification method
        // which can be overridden by implementations.  This means
        // implementation only need to override visitors for classes of
        // interest, and for others they can decide whether to fail, ignore, or
        // take some other action.
        pw.print("virtual void visit(");
        pw.print(getCppClassName(clazz));
        pw.println(" &)");
        pw.println("{ unhandledVisit(); }");
    }

    /**
     * Finds the JMI class corresponding to a Java interface.
     *
     * @param clazz the Java interface
     *
     * @return the corresponding JMI class, or null if clazz not in set of
     * interfaces to be generated
     */
    private RefClass toJmiClass(Class clazz)
    {
        return (RefClass) javaToJmiMap.get(clazz);
    }
}

// End ProxyGen.java
