/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package net.sf.saffron.runtime;


// REVIEW jvs 2-Mar-2004: To quote from the javadoc for java.lang.Error, "An
// Error is a subclass of Throwable that indicates serious problems that a
// reasonable application should not try to catch."  SaffronError subclasses
// Error, but violates this rule, so it should probably be changed to
// RuntimeException instead.  However, some uses of SaffronError
// (e.g. Util.newInternal, which functions like an assertion) would be better
// left subclassing from Error.

/**
 * <code>SaffronError</code> indicates a runtime error within Saffron library
 * code or generated code.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 12 November, 2001
 */
public class SaffronError extends Error
{
    /** Constructs a SaffronError with no parameters. */
    public SaffronError()
    {
        super();
    }

    /** Constructs a SaffronError with a message. */
    public SaffronError(String s)
    {
        super(s);
    }

    /** Constructs a SaffronError with the causing {@link Throwable}. */
    public SaffronError(Throwable e)
    {
        super(e);
    }

    /** Constructs a SaffronError with a message and the causing
     * {@link Throwable}. */
    public SaffronError(
        String s,
        Throwable e)
    {
        super(s, e);
    }
}


// End SaffronError.java
