/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Tech
// You must accept the terms in LICENSE.html to use this software.
//
// jhyde 20 February, 2002
*/
package openjava.ojc;
import org.apache.tools.ant.Task;

/**
 * This is a dummy implementation of <code>OpenJavaTask</code> to use during
 * boot time, because Ant requires that each <code>&lt;taskdef&gt;</code> has a
 * valid class behind it. It has few dependencies, and goes in
 * <code>boot.jar</code>. After the real <code>OpenJavaTask</code> has built
 * successfully.
 *
 * @author jhyde
 * @since 20 February, 2002
 * @version $Id$
 **/
public class OpenJavaTask extends Task {
}

// End OpenJavaTask.java
