/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Tech
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
package net.sf.saffron.trace;

import net.sf.saffron.oj.stmt.OJStatement;
import net.sf.saffron.oj.xlat.OJQueryExpander;
import net.sf.saffron.util.SaffronException;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Contains all of the {@link java.util.logging.Logger tracers} used within
 * Saffron.
 *
 * <p>This class is similar to {@link org.eigenbase.trace.EigenbaseTrace}; see
 * there for a description of how to define tracers.
 *
 * @author jhyde
 * @since May 24, 2004
 * @version $Id$
 **/
public class SaffronTrace {
    /**
     * The "net.sf.saffron.oj.OJStatement" tracer prints the generated
     * program at level {@link java.util.logging.Level#FINE} or higher.
     */
    public static Logger getStatementTracer() {
        return Logger.getLogger(OJStatement.class.getName());
    }

    /**
     * The "net.sf.saffron.oj.OJQueryExpander" tracer prints:<ul>
     * <li>the result of expanding a query ({@link Level#FINE})</li>
     * <li>if an expression changes calling conventions
     *     ({@link Level#FINE})</li>
     * <li>as each queries are recursively converted, prints the query
     *     before, and the result and its row type ({@link Level#FINE})</li>
     */
    public static Logger getQueryExpanderTracer() {
        return Logger.getLogger(OJQueryExpander.class.getName());
    }

    /**
     * The "net.sf.saffron.util.SaffronException" tracer reports when a
     * {@link SaffronException} is created
     * (at level {@link Level#FINE} or higher).
     *
     * <p>Unlike other tracers, this tracer is initialized inside
     * its class, {@link SaffronException}. This is because
     * {@link SaffronException} must not depend upon other classes.
     */
    public static Logger getExceptionTracer() {
        return SaffronException.tracer;
    }
}

// End SaffronTrace.java
