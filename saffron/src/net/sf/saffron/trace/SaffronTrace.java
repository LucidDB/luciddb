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
import net.sf.saffron.oj.rel.JavaRelImplementor;
import net.sf.saffron.util.SaffronException;
import net.sf.saffron.core.SaffronPlanner;

import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Contains all of the {@link java.util.logging.Logger tracers} used within
 * saffron.
 *
 * <h3>Note to developers</h3>
 *
 * <p>Please ensure that every tracer used in Saffron is
 * added to this class as a <em>public static final</em> member called
 * <code><i>component</i>Tracer</code>. For example,
 * {@link #getPlannerTracer} is the tracer used by all classes which take part
 * in the query planning process.
 *
 * <p>The javadoc in this file is the primary source of information on what
 * tracers are available, so the javadoc against each tracer member must be
 * an up-to-date description of what that tracer does. Be sure to describe what
 * {@link Level tracing level} is required to obtain each category of tracing.
 *
 * <p>In the class where the tracer is used, create a <em>private</em> (or
 * perhaps <em>protected</em>) <em>static final</em> member called
 * <code>tracer</code>.
 *
 * @author jhyde
 * @since May 24, 2004
 * @version $Id$
 **/
public class SaffronTrace {
    /**
     * The "net.sf.saffron.core.SaffronPlanner" tracer prints the query
     * optimization process.
     *
     * <p>Levels:<ul>
     * <li>{@link Level#FINE} prints rules as they fire;
     * <li>{@link Level#FINER} prints and validates the whole expression pool
     *     and rule queue as each rule fires;
     * <li>{@link Level#FINEST} prints finer details like rule importances.
     * </ul>
     */
    public static Logger getPlannerTracer() {
        return Logger.getLogger(SaffronPlanner.class.getName());
    }

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
     * The "net.sf.saffron.oj.rel.JavaRelImplementor" tracer reports
     * when expressions are bound to variables ({@link Level#FINE})
     */
    public static Logger getRelImplementorTracer() {
        return Logger.getLogger(JavaRelImplementor.class.getName());
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

    /**
     * The "net.sf.saffron.sql.parser" tracer reports parser events in
     * {@link net.sf.saffron.sql.parser.SqlParser} and
     * other classes (at level {@link Level#FINE} or higher).
     */
    public static final Logger parserTracer =
            getParserTracer();

    public static Logger getParserTracer() {
        return Logger.getLogger("net.sf.saffron.sql.parser");
    }
}

// End SaffronTrace.java
