/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
// jhyde, Jun 5, 2002
*/

package openjava.ptree.util;

import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.mop.Toolbox;
import openjava.ptree.*;
import openjava.tools.DebugOut;

import java.util.Hashtable;
import java.util.Vector;

/**
 * A <code>ClassMap</code> is ...
 *
 * @author jhyde
 * @since Jun 5, 2002
 * @version $Id$
 **/
public class ClassMap {
    // REVIEW jvs 21-Jun-2003:  change to HashMap?  This shouldn't be accessed
    // from unsynchronized code.
	/**
	 * Map a {@link HashableArray} (which is just a wrapper around an array of
	 * classes and names to the {@link SyntheticClass} which implements that
	 * array of types.
	 */
	Hashtable mapKey2SyntheticClass = new Hashtable();

    /**
     * Class from which synthetic classes should be subclassed.
     */
    Class syntheticSuperClass;

    // REVIEW jvs 21-Jun-2003:  I think this needs to remain static to prevent
    // conflicts between concurrent threads, right?  Then access to it needs to
    // be synchronized.
	private static int id = 0;
    
    // REVIEW jvs 21-Jun-2003:  need to make this thread-local, right?  Perhaps
    // this whole class should be unified into OJTypeFactoryImpl.
	private static ClassMap instance;

	public ClassMap(Class syntheticSuperClass) {
        this.syntheticSuperClass = syntheticSuperClass;
	}

	public static void setInstance(ClassMap _instance) {
		instance = _instance;
	}
	public static ClassMap instance() {
		return instance;
	}

	/**
	 * Creates a <code>SyntheticClass</code> with named fields.  We don't check
	 * whether there is an equivalent class -- all classes with named fields
	 * are different.
	 */
	public OJClass createProject(
			OJClass declarer, OJClass[] classes, String[] fieldNames) {
		boolean isJoin = false;
		return create(declarer, classes, fieldNames, isJoin);
	}

	private OJClass create(
			OJClass declarer, OJClass[] classes, String[] fieldNames,
			boolean isJoin) {
		if (fieldNames == null) {
			fieldNames = new String[classes.length];
		}
		assert(classes.length == fieldNames.length) :
            "SyntheticClass.create: mismatch between classes and field names";
		for (int i = 0; i < fieldNames.length; i++) {
			if (fieldNames[i] == null) {
				fieldNames[i] = SyntheticClass.makeField(i);
			}
		}
		// make description
		StringBuffer sb = new StringBuffer();
		sb.append("{");
		for (int i = 0; i < classes.length; i++) {
			if (i > 0) sb.append(", ");
			sb.append(fieldNames[i]);
			sb.append(": ");
			sb.append(classes[i].toString().replace('$', '.'));

			if (isJoin) {
				assert(!SyntheticClass.isJoinClass(classes[i])) :
                    "join classes cannot contain join classes";
			}
		}
		sb.append("}");
		String description = sb.toString();

		// is there already an equivalent SyntheticClass?
		SyntheticClass clazz = (SyntheticClass) mapKey2SyntheticClass.get(
				description);
		if (clazz == null) {
			Environment env = declarer.getEnvironment();
			String className =
					(isJoin ? SyntheticClass.JOIN_CLASS_PREFIX :
					SyntheticClass.PROJECT_CLASS_PREFIX) +
					Integer.toHexString(id++);
			ClassDeclaration decl = makeDeclaration(
					className, classes, fieldNames);
			clazz = new SyntheticClass(
					env, declarer, classes, fieldNames, decl, description);
			// register ourself
			try {
				declarer.addClass(clazz);
			} catch (openjava.mop.CannotAlterException e) {
				throw Toolbox.newInternal(
						e, "holder class must be OJClassSourceCode");
			}
			env.recordMemberClass(declarer.getName(), decl.getName());
			env.getGlobalEnvironment().record(clazz.getName(), clazz);

			DebugOut.println(
					"created SyntheticClass: name=" + clazz.getName() +
					", description=" + description);
			mapKey2SyntheticClass.put(description, clazz);
		}
		return clazz;
	}

	private ClassDeclaration makeDeclaration(
			String className, OJClass[] classes, String[] fieldNames) {
		MemberDeclarationList fieldList = new MemberDeclarationList();
		for (int i = 0; i < classes.length; i++) {
			FieldDeclaration field = new FieldDeclaration(
					new ModifierList(ModifierList.PUBLIC),
					TypeName.forOJClass(classes[i]),
					fieldNames[i], null);
			fieldList.add(field);
		}
		ModifierList modifierList = new ModifierList(
				ModifierList.PUBLIC | ModifierList.STATIC);
		ClassDeclaration classDecl = new ClassDeclaration(
				modifierList, className,
				new TypeName[]{
					TypeName.forClass(syntheticSuperClass)},
				null, fieldList);
		return classDecl;
	}

	/**
	 * Creates a <code>SyntheticClass</code>, or if there is already one with
	 * the same number and type of fields, returns that.
	 */
	public OJClass createJoin(OJClass declarer, OJClass[] classes) {
		if (classes.length == 1) {
			// don't make a singleton SyntheticClass, just return the atomic
			// class
			return classes[0];
		}
		boolean isJoin = true;
		return create(declarer, classes, null, isJoin);
	}


	/**
	 * <p>Make the type of a join.  There are two kinds of classes. A <dfn>real
	 * class</dfn> exists in the developer's environment.  A <dfn>synthetic
	 * class</dfn> is constructed by the system to describe the
	 * intermediate and final results of a query.  We are at liberty to modify
	 * synthetic classes.</p>
	 *
	 * <p>If we join class C1 to class C2, the result is a synthetic class:
	 *
	 * <pre>
	 * class SC1 {
	 *     C1 $f0;
	 *     C2 $f1;
	 * }
	 * </pre>
	 *
	 * Suppose that we now join class C3 to this; you would expect the result
	 * type to be a new synthetic class:
	 *
	 * <pre>
	 * class SC2 {
	 *     class SC1 {
	 *         C1 $f0;
	 *         C2 $f1;
	 *     } $f0;
	 *     class C3 $f1;
	 * }
	 * </pre>
	 *
	 * Now imagine the type resulting from a 6-way join.  It will be very
	 * difficult to unpick the nesting in order to reference fields or to
	 * permute the join order.  Therefore when one or both of the inputs to a
	 * join are synthetic, we break them apart and re-construct them.  Type of
	 * synthetic class SC1 joined to class C3 above is
	 *
	 * <pre>
	 * class SC3 {
	 *     C1 $f0;
	 *     C2 $f1;
	 *     C3 $f2;
	 * }
	 * </pre>
	 *
	 * <p>There are also <dfn>row classes</dfn>, which are synthetic classes
	 * arising from projections.  The type of
	 *
	 * <pre>select from (select deptno from dept)
	 *   join emp
	 *   join (select loc.nation, loc.zipcode from loc)</pre>
	 *
	 * is
	 *
	 * <pre>
	 * class SC {
	 *     int $f0;
	 *     Emp $f1;
	 *     class RC {
	 *         String nation;
	 *         int zipcode;
	 *     } $f2;
	 * }
	 * </pre>
	 *
	 * <p>This deals with nesting; we still need to deal with the field
	 * permutations which occur when we re-order joins.  A permutation operator
	 * moves fields back to their original positions, so that join transforms
	 * preserve type.</p>
	 **/
	public OJClass makeJoinType(
			OJClass declarer, OJClass left, OJClass right) {
		Vector classesVector = new Vector();
		addAtomicClasses(classesVector, left);
		addAtomicClasses(classesVector, right);
		OJClass[] classes = new OJClass[classesVector.size()];
		classesVector.copyInto(classes);
		return createJoin(declarer, classes);
	}

	private static void addAtomicClasses(Vector classesVector, OJClass clazz) {
		if (SyntheticClass.isJoinClass(clazz)) {
			OJClass[] classes = ((SyntheticClass) clazz).classes;
			for (int i = 0; i < classes.length; i++) {
				addAtomicClasses(classesVector, classes[i]);
			}
		} else {
			classesVector.addElement(clazz);
		}
	}

}

// End ClassMap.java
