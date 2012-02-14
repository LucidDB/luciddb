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

import java.io.*;

import java.math.*;

import java.nio.*;
import java.nio.charset.*;

import org.eigenbase.sql.*;
import org.eigenbase.util.*;
import org.eigenbase.util14.*;


/**
 * Represents a virtual register. Each register lives in a register set of type
 * {@link CalcProgramBuilder.RegisterSetType}<br>
 * Each register is of type {@link CalcProgramBuilder.OpType}
 *
 * @author jhyde
 * @version $Id$
 * @since Jan 11, 2004
 */
public class CalcReg
    implements CalcProgramBuilder.Operand
{
    //~ Instance fields --------------------------------------------------------

    final CalcProgramBuilder.OpType opType;
    final Object value;
    CalcProgramBuilder.RegisterSetType registerType;

    /**
     * Number of bytes storage to allocate for this value.
     */
    int storageBytes;
    int index;

    //~ Constructors -----------------------------------------------------------

    CalcReg(
        CalcProgramBuilder.OpType opType,
        Object value,
        CalcProgramBuilder.RegisterSetType registerType,
        int storageBytes,
        int index)
    {
        this.opType = opType;
        this.value = value;
        this.registerType = registerType;
        this.storageBytes = storageBytes;
        this.index = index;
    }

    //~ Methods ----------------------------------------------------------------

    final public CalcProgramBuilder.OpType getOpType()
    {
        return opType;
    }

    final CalcProgramBuilder.RegisterSetType getRegisterType()
    {
        return registerType;
    }

    final int getIndex()
    {
        return index;
    }

    final Object getValue()
    {
        return value;
    }

    /**
     * Serializes the {@link #value} in the virtual register if not null<br>
     * <b>NOTE</b> See also {@link #print} which serializes the "identity" of
     * the register
     *
     * @param writer
     * @param outputComments
     */
    void printValue(PrintWriter writer, final boolean outputComments)
    {
        if (null == value) {
            if (outputComments) {
                writer.print(CalcProgramBuilder.formatComment("<NULL>"));
            }
        } else if (value instanceof String) {
            // Convert the string to an array of bytes assuming (TODO:
            // don's assume!) latin1 encoding, then hex-encode.
            final String s = (String) value;
            final Charset charset = Charset.forName("ISO-8859-1");
            assert charset != null;
            final ByteBuffer buf = charset.encode(s);
            writer.print("0x");
            writer.print(
                ConversionUtil.toStringFromByteArray(
                    buf.array(),
                    16));
            if (outputComments) {
                writer.print(CalcProgramBuilder.formatComment(s));
            }
        } else if (value instanceof byte []) {
            writer.print("0x");
            writer.print(
                ConversionUtil.toStringFromByteArray((byte []) value, 16));
        } else if (value instanceof Boolean) {
            writer.print(((Boolean) value).booleanValue() ? "1" : "0");
        } else if (value instanceof SqlLiteral) {
            writer.print(((SqlLiteral) value).toValue());
        } else if (value instanceof BigDecimal) {
            if (opType.isExact()) {
                writer.print(value.toString());
            } else {
                writer.print(
                    Util.toScientificNotation((BigDecimal) value));
            }
        } else {
            writer.print(value.toString());
        }
    }

    /**
     * Serializes the identity of the register. It does not attempt to serialize
     * its value; see {@link #printValue} for that.
     *
     * @param writer
     */
    final public void print(PrintWriter writer)
    {
        writer.print(registerType.prefix);

        //writer.print(type.getTypeCode());
        writer.print(index);
    }
}

// End CalcReg.java
