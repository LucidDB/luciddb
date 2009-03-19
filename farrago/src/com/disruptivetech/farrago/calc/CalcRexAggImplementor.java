/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
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
package com.disruptivetech.farrago.calc;

import org.eigenbase.rex.*;


/**
 * Translates a call to an aggregate function to calculator assembly language.
 *
 * <p>Implementors are held in a {@link CalcRexImplementorTable}.
 *
 * @author jhyde
 * @version $Id$
 * @since June 2nd, 2004
 */
public interface CalcRexAggImplementor
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Generates instructions to initialize an accumulator for a call to this
     * aggregate function, and returns the register which holds the accumulator.
     *
     * <p>For example, for <code>SUM(x)</code>, this method generates <code>O
     * s8; V 0; T; MOVE O0, C0;</code> and returns the <code>O0</code> register.
     *
     * @param call The call to the aggregate function to be implemented
     * @param accumulatorRegister The accumulator register to be populated
     * @param translator Calculator code generator
     */
    void implementInitialize(
        RexCall call,
        CalcReg accumulatorRegister,
        RexToCalcTranslator translator);

    /**
     * Generates instructions to add a new value to an aggregation.
     *
     * <p>For example, for <code>SUM(x)</code>, this method generates <code>I
     * s8; O s8; T; ADD O0, I0;</code>.
     *
     * @param call The call to the aggregate function to be implemented
     * @param accumulatorRegister The accumulator register
     * @param translator Calculator code generator
     */
    void implementAdd(
        RexCall call,
        CalcReg accumulatorRegister,
        RexToCalcTranslator translator);

    /**
     * Generates instructions to initialize and add a new value to an
     * aggregation. This could call implementInitialize followed by implementAdd
     *
     * @param call The call to the aggregate function to be implemented
     * @param accumulatorRegister The accumulator register
     * @param translator Calculator code generator
     */
    void implementInitAdd(
        RexCall call,
        CalcReg accumulatorRegister,
        RexToCalcTranslator translator);

    /**
     * Generates instructions to implement this call, and returns the register
     * which holds the result.
     *
     * <p>For example, for <code>SUM(x)</code>, this method generates <code>I
     * s8; O s8; T; SUB O0, I0;</code>
     *
     * @param call The call to the aggregate function to be implemented.
     * @param accumulatorRegister The accumulator register
     * @param translator Calculator code generator
     */
    void implementDrop(
        RexCall call,
        CalcReg accumulatorRegister,
        RexToCalcTranslator translator);

    /**
     * Returns whether this implementor can handle the given call.
     *
     * @param call The call to the aggregate function to be implemented
     */
    boolean canImplement(RexCall call);
}

// End CalcRexAggImplementor.java
