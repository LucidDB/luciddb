/*
 * TranslatorThread.java
 *
 */
package openjava.ojc;


import openjava.mop.*;
import openjava.ptree.*;


public class TranslatorThread extends Thread
{
    private final Environment		env;
    private final OJClass	clazz;

    public TranslatorThread(Environment env, OJClass clazz) {
	this.env = env;
        this.clazz = clazz;
    }
  
    public void run() {
        try {
	    ClassDeclaration cdecl = clazz.getSourceCode();
	    ClassDeclaration newdecl = clazz.translateDefinition(env, cdecl);
	    if (newdecl != cdecl)  cdecl.replace( newdecl );
	} catch (Exception ex) {
	    System.err.println( "fail to translate " + clazz.getName() +
				" : " + ex);
	    ex.printStackTrace();
	}

	synchronized (clazz) {
	    OJSystem.waited = null;
	    clazz.notifyAll();
	}
    }

}
