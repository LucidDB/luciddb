/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
// jhyde, Jun 6, 2002
*/

package openjava.ojc;

import koala.dynamicjava.interpreter.InterpreterException;
import koala.dynamicjava.interpreter.TreeInterpreter;
import koala.dynamicjava.parser.wrapper.JavaCCParserFactory;
import net.sf.saffron.util.Util;

import java.io.IOException;
import java.io.StringReader;

/**
 * <code>DynamicJavaCompiler</code> implements the {@link JavaCompiler}
 * interface by calling <a href="http://koala.ilog.fr/djava/">DynamicJava</a>.
 *
 * <p> DynamicJava runs faster than a regular java compiler, does not
 * necessarily generate temporary files, and can run inside a sandbox. But
 * there are a few issues:<ul>
 *
 * <li>The classes are only accessible via DynamicJava's class loader
 *     ({@link #getClassLoader}).</li>
 *
 * <li>There seems to be a bug with access to methods in anonymous classes,
 *     for example, the <code>public</code> modifier should not be required
 *     here:<blockquote>
 *
 * <pre>int x = new Object() {
 *     <em>public</em> int five() { return 5; }
 * }.foo();</pre>
 *
 *     </blockquote></li>
 *
 * <li>Problem with fully-qualified names of inner classes. It prefers
 *     <code>pakkage.Outer$Inner</code> to <code>pakkage.Outer.Inner</code>.
 *     </li>
 *
 * </ul>
 *
 * <p> <b>Version</b>. To build and run this class, you will need to download
 * DynamicJava 1.1.4 or later and include
 * <code>${djava.home}/lib/dynamicjava.jar</code> on your class path. </p>
 **/
public class DynamicJavaCompiler implements JavaCompiler {
	private DynamicJavaCompilerArgs args = new DynamicJavaCompilerArgs();
	private TreeInterpreter interpreter = new TreeInterpreter(
			new JavaCCParserFactory());

	public DynamicJavaCompiler() {
	}

	public JavaCompilerArgs getArgs() {
		return args;
	}

	public void compile() {
		String[] names = args.getFileNames();
		assert(names.length == 1);
		String fileName = names[0];
		try {
			Object o;
			if (args.source != null) {
				o = interpreter.interpret(new StringReader(args.source), fileName);
			} else {
				o = interpreter.interpret(fileName);
			}
			Util.discard(o);
		} catch (InterpreterException e) {
			throw Util.newInternal(e, "while compiling '" + fileName + "'");
		} catch (IOException e) {
			throw Util.newInternal(e, "while compiling '" + fileName + "'");
		}
	}

	public ClassLoader getClassLoader() {
		return interpreter.getClassLoader();
	}

	private static class DynamicJavaCompilerArgs extends JavaCompilerArgs {
		String source;
		public DynamicJavaCompilerArgs() {
		}

		public void setSource(String source, String fileName) {
			this.source = source;
			addFile(fileName);
		}
	}
}

// End DynamicJavaCompiler.java
