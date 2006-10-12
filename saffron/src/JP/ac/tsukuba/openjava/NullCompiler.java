/*
 * NullCompiler.java
 *
 * Oct 10, 2000  Michiaki Tatsubori
 */
package JP.ac.tsukuba.openjava;


import java.io.*;
import org.eigenbase.javac.*;


/**
 * The class <code>NullCompiler</code> does nothing.
 * <p>
 */
public class NullCompiler implements JavaCompiler
{
    public NullCompiler() {
    }

    public static void main(String[] args) {
    }

    public int getTotalByteCodeSize()
    {
        return 0;
    }
    
	public void compile() {
	}

	public JavaCompilerArgs getArgs() {
		return new JavaCompilerArgs();
	}

	public ClassLoader getClassLoader() {
		return null;
	}
}
