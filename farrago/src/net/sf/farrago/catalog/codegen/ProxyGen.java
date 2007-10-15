/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import org.eigenbase.jmi.JmiObjUtil;
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

    private static final Comparator<Class> classNameComparator =
        new Comparator<Class>() {
            public int compare(
                Class o1,
                Class o2)
            {
                final String name1 = o1.getName();
                final String name2 = o2.getName();
                return name1.compareTo(name2);
            }
        };

    private static final Comparator<Method> methodNameComparator =
        new Comparator<Method>() {
            public int compare(Method m1, Method m2)
            {
                String name1 = mangle(m1.getName());
                String name2 = mangle(m2.getName());
                
                return name1.compareTo(name2);
            }
            
            private String mangle(String name)
            {
                if (name.length() > 3 &&
                    (name.startsWith("get") || name.startsWith("set")))
                {
                    return name.substring(3) + "_" + name.substring(0, 3);                    
                } else if (name.length() > 2 && name.startsWith("is")) {
                    return name.substring(2) + "_" + name.substring(0, 2);                                        
                } else {
                    return name;
                }
            }
        };
        
    //~ Instance fields --------------------------------------------------------

    /**
     * Map from Class to corresponding C++ type name as String.
     */
    private Map<Class, CppTypeInfo> cppTypeMap = 
        new HashMap<Class, CppTypeInfo>();

    /**
     * Map from Class to RefClass for everything in genInterfaces.
     */
    private Map<Class, RefClass> javaToJmiMap = new HashMap<Class, RefClass>();

    /**
     * Map from Class to corresponding Java type String to use in method
     * signatures.
     */
    private Map<Class, String> javaTypeMap = new HashMap<Class, String>();

    /**
     * PrintWriter used to generate output.
     */
    private PrintWriter pw;

    /**
     * Set containing all interfaces (represented as Class objects) for which
     * C++ proxies are to be generated.
     */
    private Set<Class> genInterfaces = new HashSet<Class>();

    /**
     * Set containing all base interfaces (represented as Class objects) from
     * which C++ proxies are to inherit.
     */
    private Set<Class> baseInterfaces = new HashSet<Class>();

    /**
     * Set containing interfaces (represented as Class objects) whose proxy
     * definition has not yet been generated. This is used to induce topological
     * order for the inheritance graph.
     */
    private Set<Class> undefinedInterfaces = new HashSet<Class>();

    /**
     * Set containing all interfaces (represented as Class objects) for which
     * C++ enums are to be generated.
     */
    private Set<Class> genEnums = new HashSet<Class>();
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

        cppTypeMap.put(Integer.TYPE, new CppTypeInfo("int32_t"));
        cppTypeMap.put(
            Integer.class, 
            new CppTypeInfo("int32_t", "constructJavaInteger", "int32Value"));
        javaTypeMap.put(Integer.TYPE, "I");

        cppTypeMap.put(Long.TYPE, new CppTypeInfo("int64_t"));
        cppTypeMap.put(
            Long.class,
            new CppTypeInfo("int64_t", "constructJavaLong", "int64Value"));
        javaTypeMap.put(Long.TYPE, "J");

        cppTypeMap.put(Short.TYPE, new CppTypeInfo("int16_t"));
        cppTypeMap.put(
            Short.class,
            new CppTypeInfo("int16_t", "constructJavaShort", "int16Value"));
        javaTypeMap.put(Short.TYPE, "S");

        cppTypeMap.put(Double.TYPE, new CppTypeInfo("double"));
        cppTypeMap.put(
            Double.class,
            new CppTypeInfo("double", "constructJavaDouble", "doubleValue"));
        javaTypeMap.put(Double.TYPE, "D");

        cppTypeMap.put(Float.TYPE, new CppTypeInfo("float"));
        cppTypeMap.put(
            Float.class,
            new CppTypeInfo("float", "constructJavaFloat", "floatValue"));
        javaTypeMap.put(Float.TYPE, "F");

        cppTypeMap.put(Boolean.TYPE, new CppTypeInfo("bool"));
        cppTypeMap.put(
            Boolean.class,
            new CppTypeInfo("boolean", "constructJavaBoolean", "boolValue"));
        javaTypeMap.put(Boolean.TYPE, "Z");

        cppTypeMap.put(
            String.class,
            new CppTypeInfo(
                "std::string", "constructJavaString", "constructString"));
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
        Collection<RefClass> allRefClasses = refPackage.refAllClasses();
        for (RefClass refClass : allRefClasses) {
            Class clazz = JmiObjUtil.getJavaInterfaceForRefObject(refClass);
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
        Collection<RefClass> allRefClasses = refPackage.refAllClasses();
        for (RefClass refClass : allRefClasses) {
            Class clazz = JmiObjUtil.getJavaInterfaceForRefObject(refClass);
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
        methods = toSortedArray(methods);
        for (int i = 0; i < methods.length; ++i) {
            if (isGetter(methods[i])) {
                generateMethodDeclaration(methods[i]);
            } else if (isSetter(methods[i])) {
                generateSetterMethodDeclaration(methods[i]);
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
            methods = toSortedArray(methods);
            for (int j = 0; j < methods.length; ++j) {
                if (isGetter(methods[j])) {
                    generateMethodDefinition(methods[j]);
                    pw.println();
                } else if (isSetter(methods[j])) {
                    generateSetterMethodDefinition(methods[j]);
                    pw.println();
                }
            }
        }
    }

    /**
     * Converts collection into an array of classes sorted by name. This is
     * necessary in order to make the output deterministic.
     */
    private static Class [] toSortedArray(Collection<Class> collection)
    {
        Class [] classes = collection.toArray(new Class[collection.size()]);
        Arrays.sort(classes, classNameComparator);
        return classes;
    }
    
    /** 
     * Sorts an array of Methods.  This is necessary in order to make output
     * deterministic. Necessary because, for instance, 
     * {@link Class#getDeclaredMethods()} does not guarantee that it returns 
     * methods in any particular order, and in practice the order changes
     * across versions (vendors?) of javac.
     */
    private static Method[] toSortedArray(Method[] methods)
    {
        Arrays.sort(methods, methodNameComparator);
        return methods;
    }

    /**
     * Generates the C++ declaration for one (getter) method.
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
     * Generates the C++ declaration for one setter method and its
     * corresponding clear method (if any).  Only setters that take
     * non-primitive types get clear methods.
     *
     * @param method .
     */
    public void generateSetterMethodDeclaration(Method method)
    {
        CppTypeInfo cppTypeInfo = 
            getCppParameterTypeInfo(method.getParameterTypes()[0]);
        String cppTypeName = cppTypeInfo.cppTypeName;
        
        pw.print("void ");
        pw.print(method.getName());
        pw.print("(const ");
        pw.print(cppTypeName);
        pw.println(" &valueRef);");

        if (!cppTypeInfo.isPrimitive) {
            pw.print("void clear");
            pw.print(method.getName().substring(3));
            pw.println("();");
        }
        
        // TODO:  make private?
        pw.print("static jmethodID meth_");
        pw.print(method.getName());
        pw.println(";");
    }

    /**
     * Generates the C++ definition for one (getter) method.
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
        CppTypeInfo cppTypeInfo = cppTypeMap.get(returnType);
        if (cppTypeInfo != null) {
            if (cppTypeInfo.isPrimitive) {
                pw.print("return pEnv->Call");
                pw.print(Character.toUpperCase(returnType.getName().charAt(0)));
                pw.print(returnType.getName().substring(1));
                pw.print("Method(jObject,meth_");
                pw.print(method.getName());
                pw.println(");");
            } else {
                // convert a jobject into a C++ type (std::string or
                // a numeric such as int64_t)
                pw.print("return ");
                pw.print(cppTypeInfo.unboxingHelperMethod);
                pw.print("(");
                pw.print("pEnv->CallObjectMethod(jObject,meth_");
                pw.print(method.getName());
                pw.println("));");
            }
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
     * Generates the C++ definition for one setter method and its 
     * corresponding clear method (if any).
     *
     * @param method .
     */
    public void generateSetterMethodDefinition(Method method)
    {
        Class<?> paramType = method.getParameterTypes()[0];
        CppTypeInfo cppTypeInfo = getCppParameterTypeInfo(paramType);
        
        pw.print("void ");
        pw.print(getCppClassName(method.getDeclaringClass()));
        pw.print("::");
        pw.print(method.getName());
        pw.print("(const ");
        pw.print(cppTypeInfo.cppTypeName);
        pw.println(" &valueRef)");
        pw.println("{");

        // convert C++ type into a jobject
        pw.print("pEnv->CallVoidMethod(jObject,meth_");
        pw.print(method.getName());
        pw.print(",");
        pw.print(cppTypeInfo.boxingHelperMethod);
        pw.println("(valueRef));");
        pw.println("}");
        
        if (!cppTypeInfo.isPrimitive) {
            pw.print("void ");
            pw.print(getCppClassName(method.getDeclaringClass()));
            pw.print("::clear");
            pw.print(method.getName().substring(3));
            pw.println("()");
            pw.println("{");

            pw.print("pEnv->CallVoidMethod(jObject,meth_");
            pw.print(method.getName());
            pw.println(",NULL);");
            pw.println("}");            
        }
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
            JmiObjUtil.getSubPackage(
                rootPackage,
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

        return ReflectUtil.getUnqualifiedClassName(clazz).replaceFirst(
            prefix,
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
        Class<?> returnType = method.getReturnType();
        if (Collection.class.isAssignableFrom(returnType)) {
            // strip off "get"
            String attrName = method.getName().substring(3);

            // determine the name of the class on the other end of the
            // association
            RefClass refClass = toJmiClass(method.getDeclaringClass());
            MofClass mofClass = (MofClass) refClass.refMetaObject();
            for (Object obj : mofClass.getContents()) {
                if (obj instanceof Reference) {
                    Reference reference = (Reference) obj;
                    String endName = reference.getReferencedEnd().getName();
                    if (endName.equalsIgnoreCase(attrName)) {
                        return "SharedProxy"
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
        CppTypeInfo cppTypeInfo = cppTypeMap.get(returnType); 
        if (cppTypeInfo != null) {
            return cppTypeInfo.cppTypeName;
        }
        return getCppRefName(returnType);
    }

    private CppTypeInfo getCppParameterTypeInfo(Class<?> parameterType)
    {
        CppTypeInfo cppTypeInfo = cppTypeMap.get(parameterType);
        if (cppTypeInfo != null) {
            return cppTypeInfo;
        }

        throw new AssertionError("Unsupported type: " + parameterType);
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
        return (methodName.startsWith("get") || methodName.startsWith("is"))
            && (method.getParameterTypes().length == 0);
    }

    /**
     * Decides whether a Java method is setter for a JMI attribute that should
     * be proxied.  Currently setter support is limited to properties with
     * names that begin with "result" and that are a primitive type, a type
     * that boxes a primitive (e.g. {@link Long}), or {@link String}.
     * We ignore all methods that don't match.
     * 
     * @param method the Java method
     * 
     * @return true iff we consider it a setter
     */
    private boolean isSetter(Method method)
    {
        String methodName = method.getName();
        return 
            methodName.startsWith("setResult") && 
            method.getReturnType() != Void.class && 
            method.getParameterTypes().length == 1 && 
            cppTypeMap.containsKey(method.getParameterTypes()[0]);
    }
    
    private String getJavaTypeSignature(Class clazz)
    {
        String s = javaTypeMap.get(clazz);
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
        methods = toSortedArray(methods);
        for (int i = 0; i < methods.length; ++i) {
            if (isGetter(methods[i]) || isSetter(methods[i])) {
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
        methods = toSortedArray(methods);
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
            } else if (isSetter(methods[i])) {
                pw.print(getCppClassName(clazz));
                pw.print("::meth_");
                pw.print(methods[i].getName());
                pw.print(" = pEnv->GetMethodID(jClass,\"");
                pw.print(methods[i].getName());
                pw.print("\",\"(");
                pw.print(getJavaTypeSignature(methods[i].getParameterTypes()[0]));
                pw.println(")V\");");
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
        return javaToJmiMap.get(clazz);
    }
    
    private static class CppTypeInfo
    {
        public final String cppTypeName;
        public final boolean isPrimitive;
        public final String boxingHelperMethod;
        public final String unboxingHelperMethod;
        
        protected CppTypeInfo(String cppTypeName)
        {
            this.cppTypeName = cppTypeName;
            this.isPrimitive = true;
            this.boxingHelperMethod = null;
            this.unboxingHelperMethod = null;
        }
        
        protected CppTypeInfo(
            String cppTypeName,
            String boxingHelperMethod,
            String unboxingHelperMethod)
        {
            this.cppTypeName = cppTypeName;
            this.isPrimitive = false;
            this.boxingHelperMethod = boxingHelperMethod;
            this.unboxingHelperMethod = unboxingHelperMethod;
        }
    }
}

// End ProxyGen.java

