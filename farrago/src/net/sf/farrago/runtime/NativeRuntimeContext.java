/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

import java.sql.*;

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.fennel.*;
import net.sf.farrago.fennel.tuple.*;
import net.sf.farrago.type.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;


/**
 * NativeRuntimeContext integrates Fennel with FarragoRuntimeContext. Currently,
 * it supports Fennel errors.
 *
 * @author John Pham
 * @version $Id$
 */
class NativeRuntimeContext
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * ErrorLevel. Keep this consistent with fennel/exec/ErrorTarget.h
     */
    public static final int ROW_ERROR = 1000;
    public static final int ROW_WARNING = 500;

    //~ Instance fields --------------------------------------------------------

    private final FarragoRuntimeContext context;
    private final Map<String, StreamDescriptor> streamMap;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new NativeRuntimeContext instance as a wrapper around a
     * FarragoRuntimeContext.
     */
    public NativeRuntimeContext(FarragoRuntimeContext context)
    {
        this.context = context;
        this.streamMap = new HashMap<String, StreamDescriptor>();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Handles a Fennel row exception by converting Fennel data into Farrago
     * data. Delegates to {@link StreamDescriptor}.
     *
     * @see FennelJavaErrorTarget#handleRowError for a description of the
     * parameters
     */
    public Object handleRowError(
        String source,
        boolean isWarning,
        String msg,
        ByteBuffer byteBuffer,
        int index)
    {
        StreamDescriptor streamDesc = streamMap.get(source);
        if (streamDesc == null) {
            streamDesc = new StreamDescriptor(context, source);
            streamMap.put(source, streamDesc);
        }

        byteBuffer.order(ByteOrder.nativeOrder());
        return streamDesc.postError(isWarning, msg, byteBuffer, index);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * StreamDescriptor represents a unique Fennel error source. Each instance
     * has its own error tag and record format.
     */
    private class StreamDescriptor
    {
        private final FarragoRuntimeContext runtimeContext;
        private final String tag;
        private final FennelTupleData tupleData;
        private final FennelTupleAccessor tupleAccessor;
        private final String [] columnNames;
        private final Object [] columnValues;
        private final ResultSetMetaData metadata;

        /**
         * Constructs a StreamDescriptor
         *
         * @param runtimeContext reference to the runtime context
         * @param source name of the Fennel error source
         */
        public StreamDescriptor(
            FarragoRuntimeContext runtimeContext,
            String source)
        {
            this.runtimeContext = runtimeContext;
            this.tag = source + "_" + Util.getFileTimestamp();

            RelDataType rowType = runtimeContext.getRowTypeForResultSet(source);
            if (rowType == null) {
                tupleData = null;
                tupleAccessor = null;
                metadata = null;
                columnNames = null;
                columnValues = null;
                return;
            }

            FennelTupleDescriptor tupleDesc =
                FennelUtil.convertRowTypeToFennelTupleDesc(rowType);

            tupleData = new FennelTupleData(tupleDesc);
            tupleAccessor = new FennelTupleAccessor(true);
            tupleAccessor.compute(tupleDesc);
            final List<List<String>> fieldOrigins =
                Collections.nCopies(rowType.getFieldCount(), null);
            metadata = new FarragoResultSetMetaData(rowType, fieldOrigins);
            columnNames = SqlTypeUtil.getFieldNames(rowType);
            columnValues = new Object[columnNames.length];
        }

        /**
         * Builds an error record and posts an error with the runtime context
         *
         * @see FennelJavaErrorTarget#handleRowError for a description of the
         * parameters
         */
        public Object postError(
            boolean isWarning,
            String msg,
            ByteBuffer byteBuffer,
            int index)
        {
            // decoding Fennel data requires metadata
            if (metadata == null) {
                EigenbaseTrace.getStatementTracer().log(
                    Level.WARNING,
                    "failed to get metadata for '" + tag + "'");
                return null;
            }

            tupleAccessor.setCurrentTupleBuf(byteBuffer);
            tupleAccessor.unmarshal(tupleData);
            try {
                for (int i = 0; i < columnValues.length; i++) {
                    // Note: result sets are 1-indexed
                    columnValues[i] =
                        FennelTupleResultSet.getRawColumnData(
                            i + 1,
                            metadata,
                            tupleData);
                }
            } catch (SQLException ex) {
                EigenbaseTrace.getStatementTracer().log(
                    Level.WARNING,
                    "failed to get raw column data",
                    ex);
                return null;
            }

            return runtimeContext.handleRowError(
                columnNames,
                columnValues,
                new EigenbaseException(msg, null),
                index,
                tag,
                isWarning);
        }
    }
}

// End NativeRuntimeContext.java
