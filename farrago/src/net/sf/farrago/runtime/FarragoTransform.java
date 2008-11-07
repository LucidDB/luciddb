/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2008 The Eigenbase Project
// Copyright (C) 2005-2008 Disruptive Tech
// Copyright (C) 2005-2008 LucidEra, Inc.
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
package net.sf.farrago.runtime;

import java.nio.*;


/**
 * A piece of generated code must implement this interface if it is to be
 * callable from a Fennel JavaTransformExecStream wrapper.
 *
 * See {@link net.sf.farrago.query.FarragoTransformDef}, which manages the
 * construction of a FarragoTransform during statement preparation, in
 * {@link net.sf.farrago.query.FarragoPreparingStmt}.
 *
 * @author Julian Hyde, Stephan Zuercher
 * @version $Id$
 */
public interface FarragoTransform
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Binds all inputs and initializes the transform.
     *
     * This method is typically generated. It is called by
     * {@link net.sf.farrago.query.FarragoExecutableJavaStmt#execute}.
     *
     * @param connection the FarragoRuntimeContext of the query that contains
     * this transform.
     * @param farragoTransformStreamName the globally unique name of the
     * ExecStream that implements this transform.
     * @param inputBindings bindings between the transform's input streamIds and
     * the ordinal assigned to them in the stream graph
     */
    void init(
        FarragoRuntimeContext connection,
        String farragoTransformStreamName,
        InputBinding [] inputBindings);

    /**
     * Does a quantum of work. Called by Fennel's JavaTransformExecStream.
     *
     * @param outputBuffer output ByteBuffer into which tuples are marshaled
     * @param quantum the maximum number of tuples that should be processed
     * before returning (in practice this is limited to 2^32)
     *
     * @return bytes marshalled into outputBuffer; 0 means end of stream, less
     * than 0 indicates an input underflow
     */
    int execute(ByteBuffer outputBuffer, long quantum);

    /**
     * Restarts this transform's underlying TupleIter(s).
     */
    void restart();

    //~ Inner Classes ----------------------------------------------------------

    /**
     * InputBinding binds a JavaTransformExecStream input's streamId to the
     * ordinal assigned to that input by the stream graph. The InputBinding objects
     * are created by {@link net.sf.farrago.query.FarragoTransformDef#init}.
     */
    public static class InputBinding
    {
        private final String inputStreamName;
        private final int ordinal;

        public InputBinding(String inputStreamName, int ordinal)
        {
            this.inputStreamName = inputStreamName;
            this.ordinal = ordinal;
        }

        public String getInputStreamName()
        {
            return inputStreamName;
        }

        public int getOrdinal()
        {
            return ordinal;
        }
    }
}

// End FarragoTransform.java
