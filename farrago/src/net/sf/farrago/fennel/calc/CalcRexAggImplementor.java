/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.fennel.calc;

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
