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

import openjava.mop.OJClass;
import openjava.ptree.ParseTree;
import openjava.tools.parser.ParseException;


/**
 * Communicates the results of an {@link Interpreter} to the world.
 *
 * <p>
{ * {@link PrintHandler} is an implementation which writes its output to a
 * {@link java.io.Writer}.
} * </p>
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 26, 2002
 */
public interface Handler
{
    void afterExecute(
        OJClass clazz,
        Object o);

    void beforeExecute(ParseTree parseTree);

    void beforeParse();

    void onParseException(ParseException e);
}


// End Handler.java
