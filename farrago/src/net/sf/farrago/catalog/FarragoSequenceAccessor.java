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
package net.sf.farrago.catalog;

import java.sql.*;

import javax.jmi.reflect.*;

import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.resource.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * A FarragoSequenceAccessor optimizes access to sequences. A sequence generates
 * new values on a per-row basis. But a sequence is not updated after every row
 * (because that would be very slow.) Instead, an accessor reserves a large
 * cache of values which it quickly allocates.
 *
 * <p>The accessor synchronizes access so multiple clients can use the sequence
 * at the same time. However this requires clients to obtain an accessor from
 * the singleton method FarragoRepos.getSequenceAccessor()
 *
 * <p>To clean up properly after a statement is completed or the database is
 * shutdown, {@link #unreserve()} should be called to release unused values.
 *
 * <p>Due to the use of singleton sequence accessors, sequence accessors may
 * exist for a long time.
 *
 * @author John Pham
 * @version $Id$
 */
public class FarragoSequenceAccessor
    extends CompoundClosableAllocation
{
    //~ Static fields/initializers ---------------------------------------------

    public static String NEXT_VALUE_METHOD_NAME = "getNext";
    private static long MAX_RESERVATION_SIZE = 1000;

    //~ Instance fields --------------------------------------------------------

    private final FarragoRepos repos;
    private final String mofId;

    private long increment, min, max;
    private boolean cycle, ascending;

    private boolean reserved;
    private Long nextReservedValue;
    private long lastReservedValue;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a FarragoSequenceAccessor
     *
     * @param repos the farrago repository containing the sequence
     * @param sequenceMofId the id of the sequence within the repository
     */
    protected FarragoSequenceAccessor(
        FarragoRepos repos,
        String sequenceMofId)
    {
        this.repos = repos;
        mofId = sequenceMofId;

        FemSequenceGenerator sequence = getSequence();
        assert (sequence != null) : "sequence was null";
        loadSequence(sequence);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Initializes the sequence accessor from a sequence.
     *
     * @param sequence up to date sequence
     */
    synchronized private void loadSequence(FemSequenceGenerator sequence)
    {
        increment = sequence.getIncrement();
        min = sequence.getMinValue();
        max = sequence.getMaxValue();
        cycle = sequence.isCycle();
        ascending = (increment > 0);
        reserved = false;
        nextReservedValue = null;
    }

    /**
     * Deallocates unused sequence values.
     */
    synchronized public void closeAllocation()
    {
        repos.beginReposSession();
        try {
            unreserve();
        } finally {
            repos.endReposSession();
        }
        super.closeAllocation();
    }

    /**
     * Retrieves a value from the sequence, possibly reserving more values in
     * the process.
     *
     * @return the value retrieved
     *
     * @throws EigenbaseException if the sequence has no more values
     */
    synchronized public long getNext()
    {
        if (nextReservedValue == null) {
            reserve();
            if (nextReservedValue == null) {
                throw FarragoResource.instance().SequenceLimitExceeded.ex(
                    getName());
            }
        }
        long ret = nextReservedValue;
        if (ret != lastReservedValue) {
            nextReservedValue += increment;
        } else {
            nextReservedValue = null;
        }
        return ret;
    }

    /**
     * Modifies a sequence and loads updated fields.
     *
     * @param options specifies fields to be modified
     * @param dataType the data type of the sequence
     */
    synchronized public void alterSequence(
        FarragoSequenceOptions options,
        RelDataType dataType)
    {
        unreserve();
        FarragoReposTxnContext txn = repos.newTxnContext();
        try {
            txn.beginWriteTxn();
            FemSequenceGenerator sequence = getSequence();
            assert (sequence != null) : "sequence was null";
            options.alter(sequence, dataType);
            loadSequence(sequence);
            txn.commit();
        } finally {
            txn.rollback();
        }
    }

    /**
     * Reserves up to {@link #MAX_RESERVATION_SIZE} values in the sequence.
     * Updates the baseValue of a sequence in the catalog sequence to the first
     * valid unreserved value.
     *
     * <p>If the reservation was successful, then {@link #nextReservedValue}
     * will be set to a non-null value.
     */
    synchronized private void reserve()
    {
        // Do nothing if values remain in current reservation
        if (nextReservedValue != null) {
            return;
        }

        FarragoReposTxnContext txn = repos.newTxnContext();
        try {
            txn.beginWriteTxn();
            reserveInternal();
            txn.commit();
        } finally {
            // REVIEW jvs 12-Jan-2007:  need to revert transient state
            // in this class too?
            txn.rollback();
        }
    }

    synchronized private void reserveInternal()
    {
        assert (nextReservedValue == null);
        FemSequenceGenerator sequence = getSequence();
        assert (sequence != null) : "sequence was null";
        if (sequence.isExpired()) {
            return;
        }

        // Find the number of values to reserve, for example:
        //     currentBase=0, 1, 2, ..., incrementCount
        long currentBase = sequence.getBaseValue();
        long diff = ascending ? (max - currentBase) : (min - currentBase);
        long incrementCount = diff / increment;
        long reservation = Math.min(incrementCount + 1, MAX_RESERVATION_SIZE);

        nextReservedValue = currentBase;
        if (reservation == (incrementCount + 1)) {
            // need to cycle
            if (cycle) {
                long first = ascending ? min : max;
                sequence.setBaseValue(first);
            } else {
                long lastValid =
                    nextReservedValue + (incrementCount * increment);
                sequence.setBaseValue(lastValid);
                sequence.setExpired(true);
            }
        } else {
            long nextValid = nextReservedValue + (reservation * increment);
            sequence.setBaseValue(nextValid);
        }
        lastReservedValue = nextReservedValue + ((reservation - 1) * increment);
        reserved = true;
    }

    /**
     * Returns values unused by the sequence accessor to the catalog
     */
    synchronized private void unreserve()
    {
        if (!reserved) {
            return;
        }
        FarragoReposTxnContext txn = repos.newTxnContext();
        try {
            txn.beginWriteTxn();
            FemSequenceGenerator sequence = getSequence();
            if (sequence == null) {
                // NOTE: sequence was deleted
            } else if (nextReservedValue == null) {
                // No values to deallocate
            } else {
                sequence.setBaseValue(nextReservedValue);
                nextReservedValue = null;
                sequence.setExpired(false);
            }
            reserved = false;
            txn.commit();
        } finally {
            txn.rollback();
        }
    }

    /**
     * Retrieves the underlying sequence from the catalog
     *
     * @return the underlying sequence, or null if the sequence was deleted
     */
    synchronized private FemSequenceGenerator getSequence()
    {
        RefBaseObject o = repos.getMdrRepos().getByMofId(mofId);
        return (FemSequenceGenerator) o;
    }

    /**
     * Returns the name of the sequence
     */
    private String getName()
    {
        FemSequenceGenerator sequence = getSequence();
        assert (sequence != null) : "sequence was null";
        if (sequence.getName().length() > 0) {
            return sequence.getName();
        }
        SqlIdentifier tableName =
            FarragoCatalogUtil.getQualifiedName(
                sequence.getColumn().getOwner());
        return tableName.toString();
    }
}

// End FarragoSequenceAccessor.java
