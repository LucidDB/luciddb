/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

package net.sf.saffron.walden;

import org.eigenbase.relopt.RelOptConnection;
import net.sf.saffron.oj.stmt.OJStatement;
import net.sf.saffron.oj.xlat.OJQueryExpander;
import org.eigenbase.runtime.VarDecl;
import org.eigenbase.util.Util;
import openjava.mop.Environment;
import openjava.mop.OJClass;
import openjava.ptree.ClassDeclaration;
import openjava.ptree.Expression;
import openjava.ptree.ParseTree;
import openjava.tools.parser.ParseException;
import openjava.tools.parser.Parser;

import java.io.Reader;
import java.util.ArrayList;



/**
 * <code>Interpreter</code> evaluates a sequence of Saffron/Java statements.
 * If a statement is a declaration, it adds to the environment available to
 * future statements.
 * 
 * <p>
 * It writes its results to a {@link Handler}.
 * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @see net.sf.saffron.web.servlet.WaldenServlet
 * @since 26 May, 2002
 */
public class Interpreter
{
    //~ Instance fields -------------------------------------------------------

    ArrayList argumentList;
    private final RelOptConnection connection;

    //~ Constructors ----------------------------------------------------------

    public Interpreter()
    {
        argumentList = new ArrayList();
        final Class clazz;
        try {
            clazz = Class.forName("sales.SalesInMemoryConnection");
        } catch (ClassNotFoundException e) {
            throw Util.newInternal(e,
                    "Could not create interpreter's default connection");
        }
        try {
            connection = (RelOptConnection) clazz.newInstance();
        } catch (InstantiationException e) {
            throw Util.newInternal(e,
                    "Could not create interpreter's default connection");
        } catch (IllegalAccessException e) {
            throw Util.newInternal(e,
                    "Could not create interpreter's default connection");
        }
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Reads and runs commands from a reader until it is empty.
     */
    public void run(Reader in,Handler handler)
    {
        Parser parser = new Parser(in);
        argumentList = new ArrayList();
        while (true) {
            boolean b = runOne(parser,handler);
            if (!b) {
                break;
            }
        }
    }

    /**
     * Reads one statement from the parser, executes it, and prints the
     * result. Returns false if there was nothing else to run.
     */
    public boolean runOne(Parser parser,Handler handler)
    {
        handler.beforeParse();
        OJStatement.Argument [] arguments =
            (OJStatement.Argument []) argumentList.toArray(
                new OJStatement.Argument[0]);
        OJStatement statement = new OJStatement(connection);
        ClassDeclaration classDecl = statement.init(arguments);
        Environment env = statement.getEnvironment();
        ParseTree parseTree = null;
        try {
            parseTree = parser.InteractiveStatement(env);
            if (parseTree == null) {
                return false;
            }
        } catch (ParseException e) {
            handler.onParseException(e);
            return parser.getNextToken().kind != Parser.EOF;
        }
        handler.beforeExecute(parseTree);
        OJQueryExpander queryExpander = new OJQueryExpander(env, connection);
        parseTree = statement.validate(parseTree,queryExpander);
        OJClass clazz = null;
        if (parseTree instanceof Expression) {
            Expression expression = (Expression) parseTree;
            clazz = Util.getType(env,expression);
        }
        Object o = statement.evaluate(classDecl,parseTree,arguments);
        handler.afterExecute(clazz,o);
        if (o instanceof VarDecl []) {
            VarDecl [] decls = (VarDecl []) o;
            for (int i = 0; i < decls.length; i++) {
                VarDecl decl = decls[i];
                argumentList.add(
                    new OJStatement.Argument(decl.name,decl.clazz,decl.value));
            }
        }
        return true;
    }
}


// End Interpreter.java
