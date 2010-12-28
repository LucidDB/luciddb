/*
 * SunJavaCompiler.java
 * Workaround for Runtime.exec() environment handling incompatibility..
 *
 * A work based on JP.ac.tsukuba.openjava.SunJavaCompiler
 *
 * Apr 16, 1999  Michiaki Tatsubori (mt@is.tsukuba.ac.jp)
 * Oct  1, 1999  Shiro Kawai (shiro@squareusa.com)
 * Nov 22, 1999  Michiaki Tatsubori
 */
package JP.ac.tsukuba.openjava;


import java.io.*;
import org.eigenbase.javac.*;

import java.lang.reflect.Method;

/**
 * The class <code>SunJavaCompiler</code> is an adapter for Sun's javac.
 *
 * Message-Id: 19990930154627G.shiro@squareusa.com
 * <p>
 * I tried OpenJava1.0a1 on my IRIX box w/ SGI's JDK1.2
 * and had a problem to run ojc.  Somehow, Runtime.exec()
 * didn't pass all the environment variables to the invoked
 * process (more specifically, it only passed TZ).
 * Consequently the CLASSPATH env was not passed to javac kicked
 * by JP.ac.tsukuba.openjava.SunJavaCompiler.complie(), which
 * prevented ojc from finishing compilation.
 * <p>
 * So far I couldn't find exact specification about how the
 * environment variables should be treated in Java specification
 * and API documents.  I guess it may depend on platforms.
 * <p>
 * We avoided the problem by explicitly passing CLASSPATH to
 * the subprocess my modifying SunJavaCompiler class, but wondering
 * if there'd be a better way to handle it...
 */
public class SunJavaCompiler implements JavaCompiler
{
	JavaCompilerArgs args = new JavaCompilerArgs();

    public static void main( String[] args ) {
		SunJavaCompiler compiler = new SunJavaCompiler();
		compiler.getArgs().setStringArray(args);
		compiler.compile();
    }

    public int getTotalByteCodeSize()
    {
        return 0;
    }
    
	public void compile() {
		compile(args.getStringArray());
	}

	public JavaCompilerArgs getArgs() {
		return args;
	}

	public ClassLoader getClassLoader() {
		return ClassLoader.getSystemClassLoader();
	}

	private void compileExternal( String[] args ) {
	String out = null;
	PrintWriter outWriter = new PrintWriter(System.err);
	String[] envp = new String[] {
	    "CLASSPATH=" + System.getProperty("java.class.path")};
	String[] commandArray = new String[args.length + 1];
	commandArray[0] = "javac";
	System.arraycopy(args, 0, commandArray, 1, args.length);
	try {
	    Process p = Runtime.getRuntime().exec( commandArray, envp);
	    InputStream in = new BufferedInputStream( p.getErrorStream() );
	    Sucker sucker = new Sucker(in, outWriter);
	    new Thread(sucker).start();
	    p.waitFor();
	    out = sucker.toString();
	} catch (Exception e) {
	    String command = toString(commandArray);
	    throw new CompilationFailedException(
		e, "Compilation failed; command=[" + command + "]");
	} finally {
        // NOTE jvs 11-Sept-2003:  This used to be outWriter.close(), but that
        // closes System.err too!
	    outWriter.flush();
	}
	if (out.indexOf(".java:") >= 0) {
	    // typical error message:
	    // "src\saffron\runtime\Dummy_742b49.java:41: cannot resolve
	    // symbol"
	    throw new CompilationFailedException(out);
	}
    }

    private static Object compilerMutex = "compilerMutex";

    // NOTE jvs 13-Sept-2003: Portions of this method were adapted from class
    // Javac13 in apache ant.
	protected void compile( String[] args ) {
        // Attempt to invoke javac directly in-process, since it's just java
        // code.  If that doesn't work, fall back to the external invocation
        // instead.
        int result = 0;
        boolean invocationFailed = true;
        ByteArrayOutputStream capturedErrRaw = new ByteArrayOutputStream();
        PrintStream capturedErr = new PrintStream(capturedErrRaw);
        // REVIEW jvs 13-Sept-2003:  Synchronization is required since we have
        // to redirect System.err.  For many use-cases, this is overkill.  The
        // Saffron precompiler is single-threaded.  From SQL, we can often
        // guarantee that any error is internal since we should be generating
        // valid code.  However, when generated code is mixed with
        // user-specified code, errors here need to get reported back as user
        // errors.  So play it safe for now.
        synchronized(compilerMutex) {
            PrintStream savedErr = System.err;
            System.setErr(capturedErr);
            try {
                Class c = Class.forName("com.sun.tools.javac.Main");
                Object compiler = c.newInstance();
                Method compile = c.getMethod(
                    "compile",
                    new Class [] {(new String [] {}).getClass ()});
                result = ((Integer) compile.invoke
                          (compiler, new Object[] {args})).intValue();
                invocationFailed = false;
            } catch (Exception e) {
                // Suppress the exception, since we're going to fall back on
                // the external compiler.  But TODO:  configuration warning.
            } finally {
                System.setErr(savedErr);
            }
        }
        if (invocationFailed) {
            compileExternal(args);
            return;
        }
        if (result != 0) {
            capturedErr.flush();
            throw new CompilationFailedException(capturedErrRaw.toString());
        }
    }
    
    private class CompilationFailedException extends RuntimeException {
	Throwable throwable;
	public CompilationFailedException(String s) {
	    super(s);
	}
	public CompilationFailedException(Throwable throwable, String s) {
	    super(s);
	    throwable.fillInStackTrace();
	    this.throwable = throwable;
	}
	public String getMessage() {
	    if (throwable == null) {
		return super.getMessage();
	    } else {
		return super.getMessage() + "; exception=[" + throwable.getMessage() + "]";
	    }
	}
    }

    private static String toString(String[] strs) {
        StringBuffer buf = new StringBuffer();
	for (int i = 0; i < strs.length; ++i) {
	    if (i > 0) {
		buf.append(" ");
	    }
	    buf.append(strs[i]);
	}
	return buf.toString();
    }

    /** A <code>Sucker</code> collects the output from a reader in a string
     * buffer. If a print writer is specified, it also writes the output there
     * (like the UNIX <code>tee</code> command).
     */
    static class Sucker implements Runnable {
	InputStream is;
	PrintWriter pw;
	StringWriter sw = new StringWriter();
	/**
	 * Constructs a <code>Sucker</code>
	 * @param is stream to read from
	 * @param pw writer to echo output to (may be null)
	 */
	Sucker(InputStream is, PrintWriter pw) {
	    this.is = is;
	    this.pw = pw;
	}
	public void run() {
	    try {
		InputStreamReader reader = new InputStreamReader(is);
		final int len = 2000;
		char b[] = new char[len];
		int bytesRead;
		while ((bytesRead = reader.read(b)) >= 0) {
		    write(b, bytesRead);
		}
	    } catch (IOException e) {
		write("Received exception: " + e.getMessage());
	    }
	}
	private void write(char[] b, int bytesRead) {
	    sw.write(b, 0, bytesRead);
	    if (pw != null) {
		pw.write(b, 0, bytesRead);
	    }
	}
	private void write(String s) {
	    sw.write(s);
	    if (pw != null) {
		pw.write(s);
	    }
	}
	public String toString() {
	    return sw.toString();
	}
    }
}
