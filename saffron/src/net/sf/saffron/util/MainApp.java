/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.util;

/**
 * Abstract base class for a Java application invoked from the command-line.
 * 
 * <p>
 * Example usage:
 * <blockquote>
 * <pre>public class MyClass extends MainApp {
 *     public static void main(String[] args) {
 *         new MyClass(args).run();
 *     }
 *     public void mainImpl() {
 *         System.out.println("Hello, world!");
 *     }
 * }</pre>
 * </blockquote>
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since Aug 31, 2003
 */
public abstract class MainApp
{
    //~ Instance fields -------------------------------------------------------

    protected final String [] args_;
    private OptionsList options_ = new OptionsList();
    private int exitCode_;

    //~ Constructors ----------------------------------------------------------

    protected MainApp(String [] args)
    {
        args_ = args;
        exitCode_ = 0;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Does the work of the application. Derived classes must implement this
     * method; they can throw any exception they like, and {@link #run} will
     * clean up after them.
     */
    public abstract void mainImpl() throws Exception;

    /**
     * Does the work of the application, handles any errors, then calls {@link
     * System#exit} to terminate the application.
     */
    public final void run()
    {
        try {
            initializeOptions();
            mainImpl();
        } catch (Throwable e) {
            handle(e);
        }
        System.exit(exitCode_);
    }

    /**
     * Sets the code which this program will return to the operating system.
     *
     * @param exitCode Exit code
     *
     * @see System#exit
     */
    public void setExitCode(int exitCode)
    {
        exitCode_ = exitCode;
    }

    /**
     * Handles an error. Derived classes may override this method to provide
     * their own error-handling.
     *
     * @param throwable Error to handle.
     */
    public void handle(Throwable throwable)
    {
        throwable.printStackTrace();
    }

    public void parseOptions(OptionsList.OptionHandler values)
    {
        options_.parse(args_);
    }

    /**
     * Initializes the application.
     */
    protected void initializeOptions()
    {
        options_.add(
            new OptionsList.BooleanOption(
                "-h",
                "help",
                "Prints command-line parameters",
                false,
                false,
                false,
                null));
    }
}


// End MainApp.java
