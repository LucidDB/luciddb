/*
// $Id$
// Saffron preprocessor and data engine
// Copyright (C) 2002-2004 Disruptive Technologies, Inc.
// Copyright (C) 2002-2004 John V. Sichi
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

package net.sf.saffron.oj.rex;

import net.sf.saffron.rex.*;

import openjava.ptree.*;

/**
 * OJRexImplementor translates a call to a particular operator to OpenJava
 * code.
 *
 * <p>Implementors are held in a {@link OJRexImplementorTable}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface OJRexImplementor
{
    /**
     * Implements a single {@link RexCall}.
     *
     * @param translator provides translation context
     *
     * @param call the call to be translated
     *
     * @param operands call's operands, which have already
     * been translated independently
     */
    public Expression implement(
        RexToOJTranslator translator,
        RexCall call,
        Expression [] operands);

    /**
     * Tests whether it is possible to implement a call.
     *
     * @param call the call for which translation is being considered
     *
     * @return whether the call can be implemented
     */
    public boolean canImplement(RexCall call);
}

// End OJRexImplementor.java
