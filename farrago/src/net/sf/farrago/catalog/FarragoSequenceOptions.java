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
package net.sf.farrago.catalog;

import java.util.*;

import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.resource.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * A class for tracking sequence generator options.
 *
 * @author John Pham
 * @version $Id$
 */
public class FarragoSequenceOptions
{
    //~ Enums ------------------------------------------------------------------

    private enum OptionType
    {
        START, INCREMENT, MINVALUE, MAXVALUE, CYCLE;
    }

    //~ Instance fields --------------------------------------------------------

    private String name;
    private Properties props;
    private boolean generatedAlways;
    private long lowerLimit, upperLimit;
    private RelDataType dataType;

    //~ Constructors -----------------------------------------------------------

    public FarragoSequenceOptions(String name)
    {
        this.name = name;
        props = new Properties();
    }

    //~ Methods ----------------------------------------------------------------

    public void setGeneratedAlways(boolean value)
    {
        generatedAlways = value;
    }

    public boolean getGeneratedAlways()
    {
        return generatedAlways;
    }

    public void setStart(Long value)
    {
        setOption(OptionType.START, value);
    }

    public Long getStart()
    {
        return (Long) getOption(OptionType.START);
    }

    public void setIncrement(Long value)
    {
        setOption(OptionType.INCREMENT, value);
    }

    public Long getIncrement()
    {
        return (Long) getOption(OptionType.INCREMENT);
    }

    public void setMin(Long value)
    {
        setOption(OptionType.MINVALUE, value);
    }

    public Long getMin()
    {
        return (Long) getOption(OptionType.MINVALUE);
    }

    private long getMinResolved(long minDefault)
    {
        Long minOption = getMin();
        return (minOption == null) ? minDefault : minOption;
    }

    public void setMax(Long value)
    {
        setOption(OptionType.MAXVALUE, value);
    }

    public Long getMax()
    {
        return (Long) getOption(OptionType.MAXVALUE);
    }

    private long getMaxResolved(long maxDefault)
    {
        Long maxOption = getMax();
        return (maxOption == null) ? maxDefault : maxOption;
    }

    public void setCycle(Boolean value)
    {
        setOption(OptionType.CYCLE, value);
    }

    public Boolean getCycle()
    {
        return (Boolean) getOption(OptionType.CYCLE);
    }

    private void setOption(OptionType opt, Object value)
    {
        if (isSet(opt)) {
            FarragoResource.instance().ValidatorDuplicateSequenceOption.ex(
                opt.toString(),
                name);
        }
        props.put(opt, value);
    }

    private Object getOption(OptionType opt)
    {
        return props.get(opt);
    }

    private boolean isSet(OptionType opt)
    {
        return props.containsKey(opt);
    }

    /**
     * Initialize a newly created sequence
     */
    public void init(
        FemSequenceGenerator sequence,
        RelDataType dataType)
    {
        applyTo(sequence, dataType, true);
    }

    /**
     * Alter an existing sequence based upon options
     */
    public void alter(
        FemSequenceGenerator sequence,
        RelDataType dataType)
    {
        applyTo(sequence, dataType, false);
    }

    /**
     * Apply options to a sequence
     *
     * @param sequence the sequence to be modified
     * @param dataType the data type of the sequence
     * @param create whether to begin with existing sequence
     */
    private void applyTo(
        FemSequenceGenerator sequence,
        RelDataType dataType,
        boolean create)
    {
        validateType(dataType);

        Long start;
        long increment, min, max;
        boolean cycle, expired;
        if (create) {
            // set most default values, except start,
            // which is based on other values
            start = null;
            increment = 1L;
            min = 0L;
            max = upperLimit;
            cycle = false;
            expired = false;
        } else {
            // load values from existing sequence
            start = sequence.getBaseValue();
            increment = sequence.getIncrement();
            min = sequence.getMinValue();
            max = sequence.getMaxValue();
            cycle = sequence.isCycle();
            expired = sequence.isExpired();
        }

        // apply options and defaults
        Enumeration<Object> keys = props.keys();
        while (keys.hasMoreElements()) {
            OptionType key = (OptionType) keys.nextElement();
            switch (key) {
            case START:
                start = getStart();
                expired = false;
                break;
            case INCREMENT:
                increment = getIncrement();
                break;
            case MINVALUE:
                min = getMinResolved(0L);
                break;
            case MAXVALUE:
                max = getMaxResolved(upperLimit);
                break;
            case CYCLE:
                cycle = getCycle();
                break;
            default:
                Util.permAssert(
                    false,
                    "invalid sequence option");
            }
        }
        if (create && (start == null)) {
            start = (increment > 0) ? min : max;
        }

        // validate values
        validateValue(start);
        validateValue(increment);
        validateValue(min);
        validateValue(max);

        // allow alter sequence to reenable an expired sequence
        if (expired) {
            long nextVal = start + increment;
            boolean sufficientRange =
                (increment > 0) ? (nextVal <= max) : (nextVal >= min);
            if (sufficientRange) {
                start = nextVal;
                expired = false;
            } else if (cycle) {
                start = (increment > 0) ? min : max;
                expired = false;
            }
        }

        if (increment == 0) {
            throw FarragoResource.instance().ValidatorZeroSequenceIncrement.ex(
                name);
        }
        if (min > max) {
            throw FarragoResource.instance().ValidatorInvalidSequenceMin.ex(
                min,
                max);
        }
        if ((min > start) || (start > max)) {
            throw FarragoResource.instance().ValidatorInvalidSequenceStart.ex(
                start,
                min,
                max);
        }

        // initialize sequence
        sequence.setBaseValue(start);
        sequence.setIncrement(increment);
        sequence.setMinValue(min);
        sequence.setMaxValue(max);
        sequence.setCycle(cycle);
        sequence.setExpired(expired);
    }

    /**
     * Validates and sets data type of sequence
     */
    private void validateType(RelDataType dataType)
    {
        if (!SqlTypeUtil.isExactNumeric(dataType)) {
            throw FarragoResource.instance().ValidatorInexactSequenceType.ex(
                name);
        }
        int precision = dataType.getPrecision();
        if (precision > SqlTypeName.MAX_NUMERIC_PRECISION) {
            // allow the validator to catch this error later
            return;
        }
        if (dataType.getScale() != 0) {
            throw FarragoResource.instance().ValidatorScaleMustBeZero.ex(
                name);
        }

        lowerLimit = SqlTypeUtil.getMinValue(dataType);
        upperLimit = SqlTypeUtil.getMaxValue(dataType);
        this.dataType = dataType;
    }

    /**
     * Validates that value is within range of specified type
     */
    private void validateValue(long value)
    {
        if ((value < lowerLimit) || (value > upperLimit)) {
            throw FarragoResource.instance().ParameterValueOutOfRange.ex(
                Long.toString(value),
                dataType.toString());
        }
    }
}

// End FarragoSequenceOptions.java
