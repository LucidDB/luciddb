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

package net.sf.saffron.oj;

import java.util.HashMap;

import javax.sql.DataSource;

import openjava.mop.Environment;
import openjava.mop.OJSystem;
import openjava.ptree.*;

import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.relopt.RelOptConnection;
import org.eigenbase.util.Util;


/**
 * Collection of saffron connections, and the expression by which they can be
 * accessed.
 *
 * <p>You only need to register connections when you are compiling code for a
 * later time.</p>
 *
 * <p>If you create a wrapper around a connection, you will need to register
 * the new connection as equivalent to the old one by calling {@link
 * #setEquivalent}.</p>
 *
 * <p>The current implementation keeps connections around for ever.
 * <b>TODO: Use weak maps</b>.</p>
 *
 * @author jhyde
 * @since Nov 28, 2003
 * @version $Id$
 **/
public class OJConnectionRegistry
{
    public static final OJConnectionRegistry instance =
        new OJConnectionRegistry();
    private HashMap map = new HashMap();
    private int counter = 0;
    private HashMap tokenMap = new HashMap();
    private HashMap equivMap = new HashMap();

    private OJConnectionRegistry()
    {
    }

    /**
     * Registers a connection with an expression and environment
     *
     * @param connection RelOptConnection
     * @param expr Java expression with which to access the connection in a
     *   generated program
     * @param jdbcExprFunctor Functor which converts an expression for the
     *   {@link RelOptConnection} into an expression for the {@link DataSource}
     *   for a JDBC connection.
     *   If null, the default functor converts <code>{expr}</code> into
     *   <code>(DataSource) {expr}</code>.
     *
     * @pre connection != null
     * @pre expr != null
     * @pre env != null
     */
    public synchronized void register(
        RelOptConnection connection,
        Expression expr,
        ExpressionFunctor jdbcExprFunctor,
        Environment env)
    {
        Util.pre(connection != null, "connection != null");
        Util.pre(expr != null, "expr != null");
        Util.pre(env != null, "env != null");
        final String token = Integer.toString(counter++);
        registerInternal(connection, expr, jdbcExprFunctor, env, token);
    }

    private ConnectionInfo registerInternal(
        RelOptConnection connection,
        Expression expr,
        ExpressionFunctor jdbcExprFunctor,
        Environment env,
        String token)
    {
        if (jdbcExprFunctor == null) {
            jdbcExprFunctor =
                new ExpressionFunctor() {
                        public Expression go(
                            Expression conExpr,
                            Expression jdbcConStrExpr)
                        {
                            return new CastExpression(
                                TypeName.forClass(DataSource.class),
                                conExpr);
                        }
                    };
        }
        final ConnectionInfo info =
            new ConnectionInfo(connection, expr, jdbcExprFunctor, env, token);
        map.put(connection, info);
        tokenMap.put(token, connection);
        return info;
    }

    /**
     * Retrieves information about a previously registered connection.
     *
     * <p>If no information is found about this connection, consults connections
     * which it has been declared to be equivalent to using
     * {@link #setEquivalent}.</p>
     */
    public synchronized ConnectionInfo get(RelOptConnection connection)
    {
        do {
            final ConnectionInfo info = (ConnectionInfo) map.get(connection);
            if (info != null) {
                return info;
            }

            // No info for this connection; look for an equivalent connection.
            connection = (RelOptConnection) equivMap.get(connection);
        } while (connection != null);
        return null;
    }

    /**
     * Retrieves information about a previously registered connection,
     * optionally registering a connection if it is not already registered.
     *
     * <p>If the connection has not been registered, and <code>create</code> is
     * true, creates an expression which references this registry. An example
     * expression would be<blockquote>
     *
     * <pre>OJConnectionRegistry.instance.get("45")</pre>
     *
     * </blockquote>where <code>"45"</code> is a token value. Note that this
     * expression will only be good within this JVM instance, and is therefore
     * no good for generated code or code which will be run in another JVM.</p>
     *
     * @param connection Connection to lookup
     * @param create Whether to create a record if connection is not known.
     * @return Information about the connection, null if the connection has not
     *   been registered and create is false
     */
    public synchronized ConnectionInfo get(
        RelOptConnection connection,
        boolean create)
    {
        ConnectionInfo info = get(connection);
        if ((info == null) && create) {
            // Returns an expression like:
            //   OJConnectionRegistry.instance.get("45")
            final String token = Integer.toString(counter++);
            final CastExpression expr =
                new CastExpression(
                    TypeName.forClass(connection.getClass()),
                    new MethodCall(
                        new FieldAccess(
                            TypeName.forClass(getClass()),
                            "instance"),
                        "get",
                        new ExpressionList(Literal.makeLiteral(token))));
            info =
                registerInternal(connection, expr, null, OJSystem.env, token);
        }
        return info;
    }

    /**
     * Retrieves a connection based upon a token. If no connection has been
     * registered with this token, throws an error.
     */
    public synchronized RelOptConnection get(String token)
    {
        final RelOptConnection saffronConnection =
            (RelOptConnection) tokenMap.get(token);
        if (saffronConnection == null) {
            throw Util.newInternal("No connection is registered with token '"
                + token + "'");
        }
        return saffronConnection;
    }

    /**
     * Retrieves a connection based upon an Openjava expression. If the
     * expression looks like<blockquote>
     *
     * <pre>OJConnectionRegistry.instance.get("45")</pre>
     *
     * </blockquote>returns the same connection as <code>get("45")</code>,
     * otherwise returns <code>null</code>.
     */
    public synchronized RelOptConnection get(ParseTree expr)
    {
        if (expr instanceof MethodCall) {
            MethodCall call = (MethodCall) expr;
            if (call.getName().equals("get")
                    && call.getReferenceExpr().equals(
                        new FieldAccess(
                            TypeName.forClass(getClass()),
                            "instance")) && (call.getArguments().size() == 1)
                    && call.getArguments().get(0) instanceof Literal) {
                final Literal literal = (Literal) call.getArguments().get(0);
                final Object value = OJUtil.literalValue(literal);
                if (value instanceof String) {
                    return get((String) value);
                }
            }
        }
        return null;
    }

    /**
     * Registers that a connection is equivalent to another.
     * @param connection  The new connection
     * @param equiv       The existing connection it is equivalent to
     */
    public void setEquivalent(
        RelOptConnection connection,
        RelOptConnection equiv)
    {
        equivMap.put(connection, equiv);
    }

    /**
     * Properties of a {@link RelOptConnection}.
     */
    public static class ConnectionInfo
    {
        public final RelOptConnection connection;
        public Expression expr;
        public ExpressionFunctor jdbcExprFunctor;
        public Environment env;
        public final String token;

        private ConnectionInfo(
            RelOptConnection connection,
            Expression expr,
            ExpressionFunctor jdbcExprFunctor,
            Environment env,
            String token)
        {
            this.connection = connection;
            this.expr = expr;
            this.jdbcExprFunctor = jdbcExprFunctor;
            this.env = env;
            this.token = token;
        }

        /**
         * Given an expression for the JDBC connect string, yields an
         * expression for the a JDBC data source.
         */
        public Expression getDataSourceExpr(Expression jdbcConStrExpr)
        {
            return this.jdbcExprFunctor.go(this.expr, jdbcConStrExpr);
        }
    }

    /**
     * Yields an expression for a JDBC data source when given expressions for
     * the Saffron connection and a JDBC connect string.
     */
    public interface ExpressionFunctor
    {
        Expression go(
            Expression conExpr,
            Expression jdbcConStrExpr);
    }
}


// End OJConnectionRegistry.java
