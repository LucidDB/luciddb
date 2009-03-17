/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package com.lucidera.runtime;

import com.lucidera.farrago.*;

import java.io.*;

import java.lang.reflect.*;

import java.text.*;

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;

import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.*;


/**
 * LucidDbRuntimeContext applies LucidDb semantics for query execution.
 *
 * @author John Pham
 * @version $Id$
 */
public class LucidDbRuntimeContext
    extends FarragoRuntimeContext
{
    //~ Static fields/initializers ---------------------------------------------

    private static final String SUMMARY_FILENAME = "Summary";
    private static final String LOG_FILENAME_EXTENSION = ".log";
    private static final String LOG_FILE_FIELD_DELIMITER = ",";
    private static final String LOG_FILE_LINE_DELIMITER = "\n";
    private static final String TIMESTAMP_FIELD_NAME = "LE_TIMESTAMP";
    private static final String EXCEPTION_FIELD_NAME = "LE_EXCEPTION";
    private static final String POSITION_FIELD_NAME = "LE_TARGET_COLUMN";
    private static final String LEVEL_FIELD_NAME = "LE_LEVEL";
    private static final String LEVEL_WARNING = "Warning";
    private static final String LEVEL_ERROR = "Error";
    private static final String CONDITION_FIELD_NAME = "CONDITION";
    private static final String ERRCODE_FIELD_NAME = "LE_ERROR_CODE";

    private static final Logger tracer = FarragoTrace.getRuntimeContextTracer();

    private static Map<String, SummaryLoggerRef> summaryInstanceMap =
        new HashMap<String, SummaryLoggerRef>();

    //~ Instance fields --------------------------------------------------------

    private final Map<String, RelDataType> iterCalcTypeMap;
    private final FarragoSessionVariables vars;
    private final Map<String, ErrorLogger> loggerMap;
    private final ErrorQuota quota;
    private String processId;
    private String actionId;

    //~ Constructors -----------------------------------------------------------

    public LucidDbRuntimeContext(FarragoSessionRuntimeParams params)
    {
        super(params);
        iterCalcTypeMap = params.iterCalcTypeMap;
        vars = getSession().getSessionVariables();
        loggerMap = new HashMap<String, ErrorLogger>();

        // Initialize error quota
        Integer errorMax = vars.getInteger(LucidDbSessionPersonality.ERROR_MAX);
        if (errorMax == null) {
            errorMax = Integer.MAX_VALUE;
        }
        Integer errorLogMax =
            vars.getInteger(LucidDbSessionPersonality.ERROR_LOG_MAX);
        if (errorLogMax == null) {
            errorLogMax = Integer.MAX_VALUE;
        }
        quota = new ErrorQuota();
        quota.errorMax = errorMax;
        quota.errorLogMax = errorLogMax;

        // Read parameters
        processId = vars.get(LucidDbSessionPersonality.ETL_PROCESS_ID);
        if (processId == null) {
            processId = "Sess" + getSession().getSessionInfo().getId();
        }
        actionId = vars.get(LucidDbSessionPersonality.ETL_ACTION_ID);
        if (actionId == null) {
            actionId = "Stmt" + stmtId;
        }
    }

    //~ Methods ----------------------------------------------------------------

    public int getTotalErrorCount()
    {
        return quota.errorCount;
    }

    public int getTotalWarningCount()
    {
        return quota.warningCount;
    }

    // override FarragoRuntimeContext
    public synchronized void closeAllocation()
    {
        if (isClosed) {
            return;
        }

        // Print a summary message if there were any errors or warnings
        if ((quota.errorCount > 0) || (quota.warningCount > 0)) {
            // NOTE jvs 9-Jan-2007: Intentionally avoid conversion to absolute
            // path in error message so that unit tests can avoid spurious
            // diffs via a relative path.  Real users should always set an
            // absolute path for properties such as logDir to ensure that error
            // messages and warnings have the full location.
            File summaryFile = getSummaryFile();
            String summaryFilename = summaryFile.getAbsolutePath();
            if (quota.errorCount <= quota.errorMax) {
                // Also post a warning (since we did not hit the limit,
                // which would have caused an excn to be thrown already,
                // making the warning superfluous and confusing).
                getWarningQueue().postWarning(
                    FarragoResource.instance().RecoverableErrorWarning.ex(
                        quota.errorCount,
                        quota.warningCount,
                        summaryFile.toString()));
            }
            SummaryLogger summary = null;
            try {
                summary = getSummaryInstance(summaryFilename);
                SummaryLogEntry entry = new SummaryLogEntry();
                for (ErrorLogger logger : loggerMap.values()) {
                    entry.PROCESS_ID = processId;
                    entry.ACTION_ID = actionId;
                    entry.ERROR_COUNT = logger.getErrorCount();
                    entry.WARNING_COUNT = logger.getWarningCount();
                    entry.FILENAME = logger.getFilename();
                    entry.SQL =
                        getSession().getSessionInfo().getExecutingStmtInfo(
                            stmtId).getSql();
                    summary.logSummary(entry);
                }
            } finally {
                if (summary != null) {
                    releaseSummaryInstance(summaryFilename);
                }
            }
        }
        super.closeAllocation();
    }

    /**
     * Gets a singleton summary logger, creating it as required
     */
    private SummaryLogger getSummaryInstance(String filename)
    {
        synchronized (summaryInstanceMap) {
            if (!summaryInstanceMap.containsKey(filename)) {
                SummaryLoggerRef ref = new SummaryLoggerRef();
                ref.logger = new SummaryLogger(filename);
                ref.refCount = 0;
                summaryInstanceMap.put(filename, ref);
            }
            SummaryLoggerRef ref = summaryInstanceMap.get(filename);
            ref.refCount++;
            return ref.logger;
        }
    }

    /**
     * Releases a singleton summary logger, possibly releasing its resources
     */
    private void releaseSummaryInstance(String filename)
    {
        synchronized (summaryInstanceMap) {
            assert (summaryInstanceMap.containsKey(filename));
            SummaryLoggerRef ref = summaryInstanceMap.get(filename);
            ref.refCount--;
            if (ref.refCount == 0) {
                ref.logger.closeAllocation();
                summaryInstanceMap.remove(filename);
            }
        }
    }

    // override FarragoRuntimeContext
    public Object handleRowError(
        SyntheticObject row,
        RuntimeException ex,
        int columnIndex,
        String tag,
        boolean isWarning)
    {
        ErrorLogger logger = getLogger(tag);
        logger.log(row, ex, columnIndex, tag, isWarning);

        // we don't return any status
        return null;
    }

    // override FarragoRuntimeContext
    public Object handleRowError(
        String [] names,
        Object [] values,
        RuntimeException ex,
        int columnIndex,
        String tag,
        boolean isWarning)
    {
        handleRowError(
            names,
            values,
            ex,
            columnIndex,
            tag,
            isWarning,
            null,
            null);
        return null;
    }

    // override FarragoRuntimeContext
    public Object handleRowError(
        String [] names,
        Object [] values,
        RuntimeException ex,
        int columnIndex,
        String tag,
        boolean isWarning,
        String errorCode,
        String columnName)
    {
        ErrorLogger logger = getLogger(tag);

        // returns defered exception or null
        return logger.log(
            names,
            values,
            ex,
            columnIndex,
            tag,
            isWarning,
            errorCode,
            columnName);
    }

    // override FarragoRuntimeContext - handles exception for rows which
    // opted to defer exceptions
    public void handleRowErrorCompletion(
        RuntimeException ex,
        String tag)
    {
        ErrorLogger logger = getLogger(tag);
        logger.completeDeferredException(ex);
    }

    /**
     * Gets the logger for a given tag. If a matching logger has not been
     * initialized yet, create a new one. Otherwise return an existing logger.
     */
    private ErrorLogger getLogger(String tag)
    {
        if (!loggerMap.containsKey(tag)) {
            String filename = getFilename(tag);
            DefaultErrorLogger logger = new DefaultErrorLogger(filename);
            addAllocation(logger);
            loggerMap.put(
                tag,
                new ErrorQuotaLogger(logger, quota));
        }
        return loggerMap.get(tag);
    }

    /**
     * Builds a filename for a logger, based on system parameters. The filename
     * will be [processId]_[actionId]_[tag][LOG_FILENAME_EXTENSION].
     *
     * <p>processId: defaults to identifier based on session id<br>
     * actionId: defaults to identifier based on statement id<br>
     * tag: often based on input/output filename, or a unique id<br>
     * LOG_FILENAME_EXTENSION: such as ".log"
     *
     * <p>For example: "1201_LoadAccount_LOCALDB.ACME.ACCOUNT_STG.log"
     */
    private String getFilename(String tag)
    {
        assert (tag != null);

        String dirName = vars.get(LucidDbSessionPersonality.LOG_DIR);
        Util.permAssert(dirName != null, "log directory was null");
        Object [] args =
            new Object[] {
                processId, actionId, tag, LOG_FILENAME_EXTENSION
            };
        String fileName = String.format("%s_%s_%s%s", args);

        File file = new File(dirName, fileName);
        return file.getPath();
    }

    /**
     * Builds a filename for the summary logger.
     */
    private File getSummaryFile()
    {
        String dirName = vars.get(LucidDbSessionPersonality.LOG_DIR);
        Util.permAssert(dirName != null, "log directory was null");

        File summaryFile =
            new File(dirName, SUMMARY_FILENAME + LOG_FILENAME_EXTENSION);
        return summaryFile;
    }

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * ErrorLogger processes error records
     */
    private interface ErrorLogger
    {
        /**
         * Writes a record to a log file
         */
        public void log(
            SyntheticObject o,
            RuntimeException ex,
            int columnIndex,
            String tag,
            boolean isWarning);

        /**
         * Writes a record to a log file
         */
        public void log(
            String [] names,
            Object [] values,
            RuntimeException ex,
            int columnIndex,
            String tag,
            boolean isWarning);

        /**
         * Writes a record to a log file
         */
        public EigenbaseException log(
            String [] names,
            Object [] values,
            RuntimeException ex,
            int columnIndex,
            String tag,
            boolean isWarning,
            String errorCode,
            String columnName);

        /**
         * Completes defered exception during record logging
         */
        public void completeDeferredException(RuntimeException ex);

        /**
         * Gets the name of the log file being written
         */
        public String getFilename();

        /**
         * Gets the number of errors processed by this logger
         */
        public int getErrorCount();

        /**
         * Gets the number of warnings processed by this logger
         */
        public int getWarningCount();
    }

    //~ Inner Classes ----------------------------------------------------------

    private class SummaryLoggerRef
    {
        SummaryLogger logger;
        int refCount;
    }

    /**
     * Base class for error loggers
     */
    private abstract class ErrorLoggerBase
        implements ErrorLogger
    {
        private int errorCount, warningCount;
        protected int fieldCount;

        // fields for logging a synthetic object
        protected SyntheticObject row;
        private Field [] fields;

        // fields for logging a names and values
        protected String [] names;
        protected Object [] values;

        /**
         * Gets the name of an IterCalcRel's output column. If the rel is being
         * used for table DML, the column name will be inferred from the table.
         * Otherwise the column name is the columnIndex.
         *
         * @param tag error handling tag used to identify an IterCalcRel
         * @param columnIndex a 1-based column index of an output column
         *
         * @return name of output column
         */
        protected String getFieldName(String tag, int columnIndex)
        {
            if (columnIndex == 0) {
                return CONDITION_FIELD_NAME;
            }
            RelDataType resultType = iterCalcTypeMap.get(tag);
            if (resultType != null) {
                assert (columnIndex <= resultType.getFieldCount());
                return resultType.getFieldList().get(columnIndex - 1).getName();
            }
            return Integer.toString(columnIndex);
        }

        // implement ErrorLogger
        public int getErrorCount()
        {
            return errorCount;
        }

        // implement ErrorLogger
        public int getWarningCount()
        {
            return warningCount;
        }

        /**
         * Writes a record to a log file
         */
        public void log(
            SyntheticObject row,
            RuntimeException ex,
            int columnIndex,
            String tag,
            boolean isWarning)
        {
            if (isWarning) {
                warningCount++;
            } else {
                errorCount++;
            }

            this.row = row;
            names = null;
            values = null;
            if (fields == null) {
                fields = row.getFields();
                fieldCount = fields.length;
                if (fields[fieldCount - 1].getName().startsWith("this$")) {
                    fieldCount--;
                }
            }
            log(ex, columnIndex, tag, isWarning, null, null);
        }

        /**
         * Writes a record to a log file
         */
        public void log(
            String [] names,
            Object [] values,
            RuntimeException ex,
            int columnIndex,
            String tag,
            boolean isWarning)
        {
            log(names, values, ex, columnIndex, tag, isWarning, null, null);
        }

        /**
         * Writes a record to a log file
         */
        public EigenbaseException log(
            String [] names,
            Object [] values,
            RuntimeException ex,
            int columnIndex,
            String tag,
            boolean isWarning,
            String errorCode,
            String columnName)
        {
            if (isWarning) {
                warningCount++;
            } else {
                errorCount++;
            }

            row = null;
            if (this.names == null) {
                this.names = names;
                fieldCount = names.length;
            }
            this.values = values;
            return log(ex, columnIndex, tag, isWarning, errorCode, columnName);
        }

        public void completeDeferredException(RuntimeException ex)
        {
            // does nothing, work done in ErrorQuotaLogger
        }

        /**
         * Writes the current log record
         */
        protected abstract EigenbaseException log(
            RuntimeException ex,
            int columnIndex,
            String tag,
            boolean isWarning,
            String errorCode,
            String columnName);

        /**
         * Gets the name of a field
         *
         * @param index zero-based column index
         */
        protected String getName(int index)
        {
            if (fields != null) {
                return fields[index].getName();
            } else {
                return names[index];
            }
        }

        /**
         * Gets the value of a field
         *
         * @param index zero-based column index
         */
        protected Object getValue(int index)
            throws IllegalAccessException
        {
            if (row != null) {
                return fields[index].get(row);
            } else {
                return values[index];
            }
        }

        /**
         * Gets an unquoted string representation of the current record
         */
        protected String getRecordString()
        {
            if (row != null) {
                return row.toString();
            } else {
                return Util.flatArrayToString(values);
            }
        }
    }

    /**
     * The default error logger writes records to a file
     *
     * <p>TODO: use existing schema export code and output BCP files as well.
     */
    private class DefaultErrorLogger
        extends ErrorLoggerBase
        implements ClosableAllocation
    {
        private static final String timestampFormatStr =
            SqlParserUtil.TimestampFormatStr;

        protected String filename;
        private final SimpleDateFormat dateFormat;
        private PrintStream ps;
        private boolean failedInit;
        private Object [] args;
        private String format;
        private boolean needsHeader;
        private boolean hasException;

        private final char LINE_DELIM = '\n';
        private final char FIELD_DELIM = ',';
        private final char QUOTE_CHAR = '"';
        private final String ONE_QUOTE = "\"";
        private final String TWO_QUOTES = "\"\"";

        public DefaultErrorLogger(String filename)
        {
            this(filename, true);
        }

        protected DefaultErrorLogger(String filename, boolean hasException)
        {
            this.filename = filename;
            dateFormat = new SimpleDateFormat(timestampFormatStr);
            this.hasException = hasException;
            failedInit = false;
        }

        // implement ClosableAllocation
        public void closeAllocation()
        {
            if (ps != null) {
                ps.flush();
                ps.close();
            }
        }

        // implement ErrorLogger
        public void log(
            RuntimeException ex,
            int columnIndex,
            String tag,
            boolean isWarning)
        {
            log(ex, columnIndex, tag, isWarning, null, null);
        }

        // implement ErrorLogger
        public EigenbaseException log(
            RuntimeException ex,
            int columnIndex,
            String tag,
            boolean isWarning,
            String errorCode,
            String columnName)
        {
            if (failedInit) {
                return null;
            }

            if (ps == null) {
                try {
                    failedInit = true;
                    boolean exists = new File(filename).exists();
                    ps = new PrintStream(new FileOutputStream(filename, true));
                    if (!exists) {
                        needsHeader = true;
                    }
                    failedInit = false;
                } catch (FileNotFoundException ex2) {
                    tracer.severe(
                        "could not open row error logger " + filename
                        + ": " + ex2);
                    return null;
                } catch (SecurityException ex2) {
                    tracer.severe(
                        "could not open row error logger " + filename
                        + ": " + ex2);
                    return null;
                }
            }

            int prefixCount = hasException ? 4 : 1;
            if (errorCode != null) {
                // row constraint udx called, logging extra error code column
                prefixCount = 5;
            }
            if (args == null) {
                args = new Object[fieldCount + prefixCount];
            }
            if (format == null) {
                StringBuffer sb = new StringBuffer("%s");
                for (int i = 1; i < args.length; i++) {
                    sb.append(LOG_FILE_FIELD_DELIMITER + "%s");
                }
                sb.append(LOG_FILE_LINE_DELIMITER);
                format = sb.toString();
            }

            args[0] = quoteValue(dateFormat.format(new Date()));
            if (hasException) {
                int i = 1;
                args[i++] =
                    quoteValue(
                        isWarning ? LEVEL_WARNING : LEVEL_ERROR);
                if (errorCode != null) {
                    // extra error code column for row constraint udx
                    args[i++] = quoteValue(errorCode);
                }
                args[i++] = quoteValue(Util.getMessages(ex));
                args[i++] =
                    (columnName == null)
                    ? quoteValue(getFieldName(tag, columnIndex))
                    : quoteValue(columnName);
            }
            for (int i = 0; i < fieldCount; i++) {
                try {
                    args[prefixCount + i] = quoteValue(getValue(i));
                } catch (IllegalAccessException ex2) {
                    tracer.severe(
                        "could not log row error field "
                        + getName(i) + ": " + ex2);
                }
            }
            if (needsHeader) {
                Object [] fieldNames = new Object[args.length];
                fieldNames[0] = quoteValue(TIMESTAMP_FIELD_NAME);
                if (hasException) {
                    int i = 1;
                    fieldNames[i++] = quoteValue(LEVEL_FIELD_NAME);
                    if (errorCode != null) {
                        // extra error code column for row constraints udx
                        fieldNames[i++] = quoteValue(ERRCODE_FIELD_NAME);
                    }
                    fieldNames[i++] = quoteValue(EXCEPTION_FIELD_NAME);
                    fieldNames[i++] = quoteValue(POSITION_FIELD_NAME);
                }
                for (int i = 0; i < fieldCount; i++) {
                    fieldNames[i + prefixCount] =
                        quoteValue(stripColumnQualifier(getName(i)));
                }
                ps.printf(format, fieldNames);
                needsHeader = false;
            }
            ps.printf(format, args);
            return null;
        }

        /**
         * Converts an object to a string then quotes it
         */
        private String quoteValue(Object o)
        {
            String s = (o == null) ? null : o.toString();
            return quoteValue(s);
        }

        /**
         * Quotes a string, if required, for standard CSV format
         */
        private String quoteValue(String s)
        {
            if (s == null) {
                return "";
            }
            if (s.length() == 0) {
                return TWO_QUOTES;
            }

            // We can simply return values without special characters.
            // In order for the quote character to be special, the value
            // must start with the quote character.
            if ((s.indexOf(LINE_DELIM) == -1)
                && (s.indexOf(FIELD_DELIM) == -1)
                && (s.charAt(0) != QUOTE_CHAR))
            {
                return s;
            }

            String replaced = s.replaceAll(ONE_QUOTE, TWO_QUOTES);
            return ONE_QUOTE + replaced + ONE_QUOTE;
        }

        /**
         * Fields of generated Java code look like: ID$0$[COLNAME] This methods
         * strips everything up to the last '$'.
         *
         * @param fieldName name of field to be cleaned stripped
         */
        private String stripColumnQualifier(String fieldName)
        {
            int lastDollar = fieldName.lastIndexOf('$');
            if (lastDollar == -1) {
                return fieldName;
            }
            return fieldName.substring(lastDollar + 1);
        }

        // implement ErrorLogger
        public String getFilename()
        {
            return filename;
        }
    }

    /**
     * SummaryLogEntry represents an entry in the summary log file. An entry
     * should be created for each error logger that encountered errors.
     */
    private class SummaryLogEntry
        extends SyntheticObject
    {
        public String PROCESS_ID;
        public String ACTION_ID;
        public int ERROR_COUNT;
        public int WARNING_COUNT;
        public String FILENAME;
        public String SQL;
    }

    /**
     * SummaryLogger create a summary of all error logs
     */
    private class SummaryLogger
        extends DefaultErrorLogger
    {
        private SummaryLogger(String filename)
        {
            super(filename, false);
        }

        /**
         * Logs an entry into the summary file. This method must be synchronized
         * as various sessions may try to write into the same summary file.
         */
        synchronized public void logSummary(SummaryLogEntry entry)
        {
            log(entry, null, 0, null, false);
        }
    }

    /**
     * An error quota for a statement
     */
    private class ErrorQuota
    {
        /**
         * Maximum errors allowed for a statement
         */
        int errorMax;

        /**
         * Maximum errors to log for a statement
         */
        int errorLogMax;

        /**
         * Ongoing count of errors
         */
        int errorCount;

        /**
         * Ongoing count of warnings
         */
        int warningCount;
    }

    /**
     * Applies a quota to an error logger
     */
    private class ErrorQuotaLogger
        extends ErrorLoggerBase
    {
        private ErrorLogger logger;
        private ErrorQuota quota;

        public ErrorQuotaLogger(ErrorLogger logger, ErrorQuota quota)
        {
            this.logger = logger;
            this.quota = quota;
        }

        // implement ErrorLogger
        public EigenbaseException log(
            RuntimeException ex,
            int columnIndex,
            String tag,
            boolean isWarning,
            String errorCode,
            String columnName)
        {
            EigenbaseException deferedException = null;
            synchronized (quota) {
                if (isWarning) {
                    quota.warningCount++;
                } else {
                    quota.errorCount++;
                }

                // NOTE: if an error causes an exception, log it only if
                // exceptions are being deferred
                if (quota.errorCount > quota.errorMax) {
                    String field =
                        (columnName == null) ? getFieldName(tag, columnIndex)
                        : columnName;
                    String row = getRecordString();
                    EigenbaseException ex2 =
                        makeRowError(ex, row, columnIndex, field);
                    if (errorCode == null) {
                        if (quota.errorMax > 0) {
                            throw FarragoResource.instance().ErrorLimitExceeded
                            .ex(
                                quota.errorMax,
                                Util.getMessages(ex2));
                        }
                        throw ex2;
                    } else {
                        deferedException = ex2;
                    }
                }

                // Log the error if the logging limit has not been reached
                if ((quota.errorCount + quota.warningCount)
                    <= quota.errorLogMax)
                {
                    if (row != null) {
                        logger.log(row, ex, columnIndex, tag, isWarning);
                    } else {
                        logger.log(
                            names,
                            values,
                            ex,
                            columnIndex,
                            tag,
                            isWarning,
                            errorCode,
                            columnName);
                    }
                }
            }
            return deferedException;
        }

        public void completeDeferredException(RuntimeException ex)
        {
            if (ex != null) {
                synchronized (quota) {
                    if ((quota.errorCount > quota.errorMax)
                        && (quota.errorMax > 0))
                    {
                        throw FarragoResource.instance().ErrorLimitExceeded.ex(
                            quota.errorMax,
                            Util.getMessages(ex));
                    }
                }
                throw ex;
            }
        }

        // implement ErrorLogger
        public String getFilename()
        {
            // Return empty filename if no logging was done. No logging was
            // done if the error limit was zero and there were no warnings,
            // or if no logging was allowed.
            if (((quota.errorMax == 0) && (quota.warningCount == 0))
                || (quota.errorLogMax == 0))
            {
                return "";
            }
            return logger.getFilename();
        }
    }
}

// End LucidDbRuntimeContext.java
