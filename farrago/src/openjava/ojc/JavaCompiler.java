/*
 * JavaCompiler.java
 *
 * Apr 16, 1999  Michiaki Tatsubori
 */
package openjava.ojc;


/**
 * The interface <code>JavaCompiler</code> represents an interface
 * to invoke a regular Java compiler.
 * Classes implementing this interface should accept the same arguments
 * as Sun's javac.
 *
 */
public interface JavaCompiler
{
    public void compile();
	public JavaCompilerArgs getArgs();
	public ClassLoader getClassLoader();
}
