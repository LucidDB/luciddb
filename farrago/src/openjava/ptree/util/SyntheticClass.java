/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
*/

package openjava.ptree.util;

import openjava.mop.*;
import openjava.ptree.*;
import org.eigenbase.util.Util;

import java.util.Enumeration;

/**
 * A <code>SyntheticClass</code> is a {@link OJClass class declaration} for
 * intermediate results in expressions. It is created implicitly while
 * expressions are being compiled.
 *
 * <p> Two synthetic classes are identical if their attributes are of the same
 * number, type, and order.
 *
 * <p> Synthetic classes are created in two ways: {@link
 * ClassMap#createProject(OJClass, OJClass[], String[])} creates the type of a
 * select clause, and {@link ClassMap#createJoin(OJClass, OJClass[])} creates
 * the type of a join. The semantics are slightly different: projection classes
 * have field names, but join classes do not; two join classes with the same
 * member types are equivalent, but two distinct projection classes may have
 * the same set of attributes.
 **/
public class SyntheticClass extends OJClass {

	/* -- Members -- */

	String description; // for debug
	OJClass[] classes;
	ClassDeclaration decl;

	public static final String JOIN_CLASS_PREFIX = "Oj_";
	public static final String PROJECT_CLASS_PREFIX = "Ojp_";
	public static final String FIELD_PREFIX = "$f";

	/* -- Constructors -- */

	public String toString() {
		return super.toString() + " " + description;
	}

	/**
	 * Called only from {@link ClassMap} methods.
	 */
	SyntheticClass(
			Environment env, OJClass declarer, OJClass[] classes,
			String[] fieldNames, ClassDeclaration decl, String description) {
		super(env, declarer, decl);
		this.classes = classes;
		this.decl = decl;
		this.description = description;

		// default constructor
		ConstructorDeclaration constructor = new ConstructorDeclaration(
				new ModifierList(ModifierList.PUBLIC),
				decl.getName(), null, null, new StatementList());
		decl.getBody().add(constructor);
		// create value constructor (unless it is the same as the default
		// constructor)
		if (classes.length > 0) {
			ParameterList parameterList = new ParameterList();
			StatementList statementList = new StatementList();
			for (int i = 0; i < classes.length; i++) {
				String varName = fieldNames[i];
				parameterList.add(
						new Parameter(TypeName.forOJClass(classes[i]), varName));
				statementList.add(
						new ExpressionStatement(
								new AssignmentExpression(
										new FieldAccess(
												SelfAccess.makeThis(),
												varName),
										AssignmentExpression.EQUALS,
										new Variable(varName))));
			}
			ConstructorDeclaration constructor2 = new ConstructorDeclaration(
					new ModifierList(ModifierList.PUBLIC),
					decl.getName(), parameterList, null, statementList);
			decl.getBody().add(constructor2);
		}
	}

	/* -- Methods -- */

	// override Object
	public boolean equals(Object o) {
		return o instanceof SyntheticClass &&
				this.description.equals(((SyntheticClass) o).description);
	}

	// override Object
	public int hashCode() {
		return HashableArray.arrayHashCode(classes);
	}

    /**
	 * Add declarations of a set of classes <code>classes</code> as inner
	 * classes of a class declaration <code>outerClassDecl</code>.
	 * Declarations which are already present are not added again.
	 **/
	public static void addMembers(
			ClassDeclaration outerClassDecl, OJClass[] classes) {
		MemberDeclarationList memberDecls = outerClassDecl.getBody();
		outer:
			for (int i = 0; i < classes.length; i++) {
				if (classes[i] instanceof SyntheticClass) {
					ClassDeclaration innerClassDecl =
							((SyntheticClass) classes[i]).decl;
					for (Enumeration existingDecls = memberDecls.elements();
						 existingDecls.hasMoreElements();) {
						if (existingDecls.nextElement() == innerClassDecl) {
							continue outer;
						}
					}
					memberDecls.add(innerClassDecl);
				}
			}
	}

    /**
	 * Creates a method in a class.
	 **/
	public static void addMethod(
			ClassDeclaration classDecl,
            StatementList statementList, String name, String[] parameterNames,
			OJClass[] parameterTypes, OJClass returnType) {
		ParameterList parameterList = new ParameterList();
		if (parameterNames.length != parameterTypes.length) {
			throw Toolbox.newInternal(
					"must have same number & type of parameters");
		}
        ModifierList modifierList;
		for (int i = 0; i < parameterNames.length; i++) {
			parameterList.add(
					new Parameter(
                            new ModifierList(),
							TypeName.forOJClass(parameterTypes[i]),
							parameterNames[i]));
		}
		MethodDeclaration methodDecl = new MethodDeclaration(
				new ModifierList(ModifierList.STATIC | ModifierList.PUBLIC),
				TypeName.forOJClass(returnType),
				name,
				parameterList,
				new TypeName[] {TypeName.forOJClass(Util.clazzSQLException)},
				statementList);
		MethodDeclaration oldMethodDecl = null;
		MemberDeclarationList body = classDecl.getBody();
		for (int i = 0, count = body.size(); i < count; i++) {
			MemberDeclaration memberDecl = body.get(i);
			if (memberDecl instanceof MethodDeclaration) {
				MethodDeclaration existingMethodDecl = (MethodDeclaration) memberDecl;
				if (existingMethodDecl.getName().equals(name) &&
						existingMethodDecl.getParameters().equals(parameterList)) {
					oldMethodDecl = existingMethodDecl;
				}
			}
		}
		if (oldMethodDecl == null) {
			body.add(methodDecl);
		} else {
			try {
				oldMethodDecl.replace(methodDecl);
			} catch (ParseTreeException e) {
				throw Util.newInternal(e, "while replacing method " + oldMethodDecl);
			}
		}
	}

	/**
	 * Converts a field name back to an ordinal.  For example,
	 * <code>getOrdinal("$f2")</code> returns 2.  If fieldName is not valid,
	 * throws an error if "fail" is true, otherwise returns -1.
	 **/
	public static int getOrdinal(String fieldName, boolean fail) {
		if (fieldName.startsWith(FIELD_PREFIX)) {
			String s = fieldName.substring(FIELD_PREFIX.length());
			try {
				return Integer.parseInt(s);
			} catch (NumberFormatException e) {
				// fall through
			}
		}
		if (fail) {
			throw Toolbox.newInternal(
					"bad field in synthetic class [" + fieldName + "]");
		} else {
			return -1;
		}
	}

	public static String makeField(int ordinal) {
		return FIELD_PREFIX + ordinal;
	}

	public static boolean isJoinClass(OJClass clazz) {
        final String name = clazz.getName(); // null for the "null class"
        return name != null && name.indexOf(JOIN_CLASS_PREFIX) >= 0;
	}

}

// End SyntheticClass.java
