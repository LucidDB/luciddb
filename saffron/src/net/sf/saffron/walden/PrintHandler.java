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

package net.sf.saffron.walden;

import java.io.PrintWriter;

import openjava.mop.OJClass;
import openjava.ptree.ParseTree;
import openjava.tools.parser.ParseException;

import org.eigenbase.runtime.VarDecl;
import org.eigenbase.util.Util;


/**
 * A <code>PrintHandler</code> writes the output of a {@link Interpreter} to a
 * {@link PrintWriter}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 26, 2002
 */
public class PrintHandler implements Handler
{
    Interpreter interpreter;
    PrintWriter pw;
    String prompt = "> ";
    boolean interactive;

    public PrintHandler(
        Interpreter interpreter,
        PrintWriter pw,
        boolean interatcive)
    {
        this.pw = pw;
        this.interpreter = interpreter;
        this.interactive = interactive;
    }

    public void afterExecute(
        OJClass clazz,
        Object o)
    {
        if (interactive) {
            pw.print("Result [" + clazz + "]: ");
            pw.flush();
        }
        if (o instanceof VarDecl []) {
            VarDecl [] decls = (VarDecl []) o;
            for (int i = 0; i < decls.length; i++) {
                VarDecl decl = decls[i];
                if (i > 0) {
                    pw.print(", ");
                }
                pw.print(decl.name);
                pw.print(": ");
                pw.print(decl.clazz.getName());
                pw.print(": ");
                Util.println(pw, decl.value);
            }
        } else if (clazz == Util.clazzVoid) {
            pw.println("void");
        } else {
            pw.print(clazz);
            pw.print(": ");
            Util.println(pw, o);
        }
    }

    public void beforeExecute(ParseTree parseTree)
    {
        if (interactive) {
            pw.print("Evaluate: ");
            pw.flush();
            pw.println(parseTree);
        }
    }

    public void beforeParse()
    {
        if (interactive) {
            pw.print(prompt);
            pw.flush();
        }
    }

    public void onParseException(ParseException e)
    {
        pw.println("Error: " + e);
    }
}


// End PrintHandler.java
