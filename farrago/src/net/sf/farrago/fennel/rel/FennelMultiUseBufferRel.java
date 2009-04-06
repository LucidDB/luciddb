/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package net.sf.farrago.fennel.rel;

import java.util.List;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;
import net.sf.farrago.type.*;

import openjava.ptree.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;


/**
 * FennelMultiUseBufferRel represents the Fennel implementation of a buffering
 * stream that's used in multiple places in a stream graph.
 *
 * <p>
 * Within the query tree, a single instance of this object can be referenced
 * multiple times.  Each reference corresponds to a node that needs to read
 * the same buffered data.  When converting this object to a streamDef, each
 * reference to the object will result in the creation of a buffer reader
 * stream.  In addition, a single buffer writer stream is created that will
 * be "shared" by the buffer readers.  Hence, there will be one buffer writer
 * stream and one or more buffer reader streams per FennelMultiUseBufferRel
 * object.
 *
 * <p>
 * The writer stream will have an incoming input stream corresponding to
 * the input into the FennelMultiUseBufferRel.  Dataflows will be created from
 * the writer stream to its corresponding reader streams.  That dataflow will
 * be used by the writer stream to communicate to the readers the first pageId
 * of the buffered data.
 *
 * <p>
 * The writer and reader streams will also share a dynamic parameter that
 * will be used to keep track of the number of active readers.
 *
 * <p>
 * Note that unlike FennelBufferRel, FennelMultiUseBufferRel only supports
 * "multipass = true" semantics.  The parameter cannot be set to false, even
 * if the buffered contents are only going to be used once.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FennelMultiUseBufferRel
    extends FennelBufferRel
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Id of the dynamic parameter that the writer will use to keep track of
     * the number of active readers.
     */
    private FennelRelParamId readerRefCountParamId;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelMultiUseBufferRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child child input
     * @param inMemory true if the buffering needs to be done only in memory
     * by a single instance of a stream
     */
    public FennelMultiUseBufferRel(
        RelOptCluster cluster,
        RelNode child,
        boolean inMemory)
    {
        super(cluster, child, inMemory, true);

        // Allocate the dynamic parameter that will be used to keep track of
        // the number of active readers
        FennelRelImplementor relImplementor =
            FennelRelUtil.getRelImplementor(child);
        readerRefCountParamId = relImplementor.allocateRelParamId();
    }

    //~ Methods ----------------------------------------------------------------

    // implement Cloneable
    public FennelMultiUseBufferRel clone()
    {
        FennelMultiUseBufferRel clone =
            new FennelMultiUseBufferRel(
                getCluster(),
                getChild().clone(),
                inMemory);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[]
                { "child", "inMemory", "readerRefCountParamId" },
            new Object[] { inMemory, readerRefCountParamId });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemBufferingTupleStreamDef bufferingStreamDef;

        FemExecutionStreamDef newWriter = null;
        FemBufferWriterStreamDef writerStreamDef = null;

        // Only one instance of this node will generate the buffered data,
        // so we assign that role to the first instance that's encountered
        // either through this call or the implementFennelChild call.  If
        // this is that instance, then we need to create the writer streamDef.
        if (implementor.isFirstTranslationInstance(this)) {
            writerStreamDef = repos.newFemBufferWriterStreamDef();
            writerStreamDef.setInMemory(inMemory);
            writerStreamDef.setMultipass(multiPass);
            writerStreamDef.setReaderRefCountParamId(
                implementor.translateParamId(
                    readerRefCountParamId).intValue());
            FarragoTypeFactory typeFactory = getFarragoTypeFactory();
            RelDataType rowType =
                typeFactory.createStructType(
                    new RelDataType[] {
                        typeFactory.createSqlType(SqlTypeName.BIGINT)
                    },
                    new String[] { "ROWCOUNT" });
            writerStreamDef.setOutputDesc(
                FennelRelUtil.createTupleDescriptorFromRowType(
                    repos,
                    typeFactory,
                    rowType));

            // If there are streams that have already been registered for this
            // node, then those are reader streamDefs.  Now that we have
            // the writer, we can add the dataflows from the writer to those
            // reader streams, as well as setting the dynamic parameter.
            List<FemExecutionStreamDef> streamDefList =
                implementor.getRegisteredStreamDefs(this);
            if (streamDefList != null) {
                for (FemExecutionStreamDef streamDef : streamDefList) {
                    FemBufferReaderStreamDef readerStreamDef =
                        (FemBufferReaderStreamDef) streamDef;
                    readerStreamDef.setReaderRefCountParamId(
                        writerStreamDef.getReaderRefCountParamId());
                    implementor.addDataFlowFromProducerToConsumer(
                        writerStreamDef,
                        streamDef);
                }
            }

            // Register the writer so it can be referenced later
            implementor.registerRelStreamDef(
                writerStreamDef,
                this,
                rowType);
            newWriter = writerStreamDef;

        // Otherwise, if a writer has already been created, retrieve it from
        // the list of already registered streamDefs.
        } else {
            List<FemExecutionStreamDef> streamDefList =
                implementor.getRegisteredStreamDefs(this);
            if (streamDefList != null) {
                for (FemExecutionStreamDef streamDef : streamDefList) {
                    if (streamDef instanceof FemBufferWriterStreamDef) {
                        writerStreamDef = (FemBufferWriterStreamDef) streamDef;
                        break;
                    }
                }
            }
        }

        // Create the reader instance of this RelNode.  Add a dataflow from
        // the writer to this reader, if the writer has already been created.
        FemBufferReaderStreamDef readerStreamDef =
            repos.newFemBufferReaderStreamDef();
        if (writerStreamDef != null) {
            implementor.addDataFlowFromProducerToConsumer(
                writerStreamDef,
                readerStreamDef);
            readerStreamDef.setReaderRefCountParamId(
                writerStreamDef.getReaderRefCountParamId());
        }
        bufferingStreamDef = readerStreamDef;
        bufferingStreamDef.setInMemory(inMemory);
        bufferingStreamDef.setMultipass(multiPass);

        // We only need to convert the child input to streamDefs for the
        // writer instance.
        if (newWriter != null) {
            FemExecutionStreamDef childInput =
                implementor.visitFennelChild((FennelRel) getChild(), 0);
            implementor.addDataFlowFromProducerToConsumer(
                childInput,
                newWriter);
        }

        return bufferingStreamDef;
    }

    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        // Only one instance of this node will generate the buffered data,
        // so we assign that role to the first instance that's encountered
        // either through this call or the toStreamDef call.
        if (implementor.isFirstTranslationInstance(this)) {
            return super.implementFennelChild(implementor);
        } else {
            return Literal.constantNull();
        }
    }
}

// End FennelMultiUseBufferRel.java
