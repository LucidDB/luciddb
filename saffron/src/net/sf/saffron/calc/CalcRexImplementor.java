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
package net.sf.saffron.calc;

import net.sf.saffron.calc.RexToCalcTranslator;
import net.sf.saffron.rex.RexCall;

/**
 * Translates a call to a particular operator to calculator assembly language.
 *
 * <p>Implementors are held in a {@link CalcRexImplementorTable}.
 *
 * @author jhyde
 * @since June 2nd, 2004
 * @version $Id$
 */
public interface CalcRexImplementor
{
    /**
     * Generates instructions to implement this call, and returns the register
     * which holds the result.
     */
    CalcProgramBuilder.Register implement(RexCall call,
            RexToCalcTranslator translator);
    boolean canImplement(RexCall call);
}

// End CalcRexImplementor.java