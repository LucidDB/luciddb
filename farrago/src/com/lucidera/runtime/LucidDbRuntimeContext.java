/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
import com.lucidera.opt.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;

import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.*;

import java.io.*;
import java.lang.reflect.*;
import java.text.*;
import java.util.*;
import java.util.logging.*;

/**
 * LucidDbRuntimeContext applies LucidDb semantics for query execution.
 *
 * @author John Pham
 * @version $Id$
 */
public class LucidDbRuntimeContext extends FarragoRuntimeContext
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

    private static final Logger tracer = 
        FarragoTrace.getRuntimeContextTracer();

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
        Integer errorMax = 
            vars.getInteger(LucidDbSessionPersonality.ERROR_MAX);
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

    // override FarragoRuntimeContext
    public void closeAllocation()
    {
        // Print a summary message if there were any errors or warnings
        if (quota.errorCount > 0 || quota.warningCount > 0) 
        {
            String summaryFilename = getSummaryFilename();
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
                    entry.SQL = getSession().getSessionInfo()
                        .getExecutingStmtInfo(stmtId).getSql();
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

    private class SummaryLoggerRef
    {
        SummaryLogger logger;
        int refCount;
    }

    /**
     * Gets a singleton summary logger, creating it as required
     */
    private SummaryLogger getSummaryInstance(String filename)
    {
        synchronized (summaryInstanceMap) {
            if (! summaryInstanceMap.containsKey(filename)) {
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
            if (ref.refCount == 0)
            {
                ref.logger.closeAllocation();
                summaryInstanceMap.remove(filename);
            }
        }
    }

    // override FarragoRuntimeContext
    public Object handleRowError(
        SyntheticObject row, RuntimeException ex, 
        int columnIndex, String tag, boolean isWarning) 
    {
        ErrorLogger logger = getLogger(tag);
        logger.log(row, ex, columnIndex, tag, isWarning);
        // we don't return any status
        return null;
    }

    // override FarragoRuntimeContext
    public Object handleRowError(
        String[] names,
        Object[] values,
        RuntimeException ex,
        int columnIndex,
        String tag,
        boolean isWarning)
    {
        ErrorLogger logger = getLogger(tag);
        logger.log(names, values, ex, columnIndex, tag, isWarning);
        // we don't return any status
        return null;
    }

    /**
     * Gets the logger for a given tag. If a matching logger has not been 
     * initialized yet, create a new one. Otherwise return an existing logger.
     */
    private ErrorLogger getLogger(String tag) 
    {
        if (! loggerMap.containsKey(tag)) {
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
     * Builds a filename for a logger, based on system parameters. The 
     * filename will be [processId]_[actionId]_[tag][LOG_FILENAME_EXTENSION].
     * 
     * <p>
     * 
     * processId: defaults to identifier based on session id<br>
     * actionId: defaults to identifier based on statement id<br>
     * tag: often based on input/output filename, or a unique id<br>
     * LOG_FILENAME_EXTENSION: such as ".log"
     * 
     * <p>
     * 
     * For example: "1201_LoadAccount_LOCALDB.ACME.ACCOUNT_STG.log"
     */
    private String getFilename(String tag) 
    {
        assert(tag != null);

        String dirName = vars.get(LucidDbSessionPersonality.LOG_DIR);
        Util.permAssert(dirName != null, "log directory was null");
        Object[] args = new Object[] {
            processId, actionId, tag, LOG_FILENAME_EXTENSION
        };
        String fileName = String.format("%s_%s_%s%s", args);

        File file = new File(dirName, fileName);
        return file.getPath();
    }

    /**
     * Builds a filename for the summary logger.
     */
    private String getSummaryFilename()
    {
        String dirName = vars.get(LucidDbSessionPersonality.LOG_DIR);
        Util.permAssert(dirName != null, "log directory was null");

        File summaryFile = 
            new File(dirName, SUMMARY_FILENAME + LOG_FILENAME_EXTENSION);
        try {
            return summaryFile.getCanonicalPath();
        } catch (IOException ex) {
            tracer.severe("Failed to get canonical path "
                + "for summary trace file: " + ex.toString());
            return summaryFile.getPath();
        }
    }

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
            String[] names,
            Object[] values,
            RuntimeException ex, 
            int columnIndex,
            String tag,
            boolean isWarning);

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

    /**
     * Base class for error loggers
     */
    private abstract class ErrorLoggerBase implements ErrorLogger
    {

        //~ Instance fields -------------------------------------------------

        private int errorCount, warningCount;
        protected int fieldCount;

        // fields for logging a synthetic object
        protected SyntheticObject row;
        private Field[] fields;

        // fields for logging a names and values
        protected String[] names;
        protected Object[] values;

        //~ Methods ---------------------------------------------------------

        /**
         * Gets the name of an IterCalcRel's output column. If the rel 
         * is being used for table DML, the column name will be inferred 
         * from the table. Otherwise the column name is the columnIndex.
         * 
         * @param tag error handling tag used to identify an IterCalcRel
         * @param columnIndex a 1-based column index of an output column
         * @return
         */
        protected String getFieldName(String tag, int columnIndex)
        {
            if (columnIndex == 0) {
                return CONDITION_FIELD_NAME;
            }
            RelDataType resultType = iterCalcTypeMap.get(tag);
            if (resultType != null) {
                assert(columnIndex <= resultType.getFieldCount());
                return resultType.getFieldList().get(columnIndex-1).getName();
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
                if (fields[fieldCount-1].getName().startsWith("this$")) {
                    fieldCount--;
                }
            }
            log(ex, columnIndex, tag, isWarning);
        }

        /**
         * Writes a record to a log file
         */
        public void log(
            String[] names,
            Object[] values,
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
            
            row = null;
            if (this.names == null) {
                this.names = names;
                fieldCount = names.length;
            }
            this.values = values;
            log(ex, columnIndex, tag, isWarning);
        }

        /**
         * Writes the current log record
         */
        protected abstract void log(
            RuntimeException ex, 
            int columnIndex,
            String tag,
            boolean isWarning);

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
                return values.toString();
            }
        }
    }

    /**
     * The default error logger writes records to a file
     * 
     * <p>TODO: use existing schema export code and output BCP 
     * files as well.
     */
    private class DefaultErrorLogger 
    extends ErrorLoggerBase implements ClosableAllocation
    {
        private static final String timestampFormatStr = 
            SqlParserUtil.TimestampFormatStr;

        protected String filename;
        private final SimpleDateFormat dateFormat;
        private PrintStream ps;
        private boolean failedInit;
        private Object[] args;
        private String format;
        private boolean needsHeader;
        private boolean hasException;

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
            if (failedInit) return;

            if (ps == null) {
                try {
                    failedInit = true;
                    boolean exists = new File(filename).exists();
                    ps = new PrintStream( 
                        new FileOutputStream(filename, true));
                    if (!exists) {
                        needsHeader = true;
                    }
                    failedInit = false;
                } catch (FileNotFoundException ex2) {
                    tracer.severe("could not open row error logger " + filename 
                        + ": " + ex2);
                    return;
                } catch (SecurityException ex2) {
                    tracer.severe("could not open row error logger " + filename 
                        + ": " + ex2);
                    return;
                }
            }

            int prefixCount = hasException ? 4 : 1;
            if (args == null) {
                args = new Object[fieldCount+prefixCount];
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
                args[1] = quoteValue(isWarning ? LEVEL_WARNING : LEVEL_ERROR);
                args[2] = quoteValue(Util.getMessages(ex));
                args[3] = quoteValue(getFieldName(tag, columnIndex));
            }
            for (int i = 0; i < fieldCount; i++) {
                try {
                    args[prefixCount+i] = 
                        quoteValue(getValue(i).toString());
                } catch (IllegalAccessException ex2) {
                    tracer.severe("could not log row error field " 
                        + getName(i) + ": " + ex2);
                }
            }
            if (needsHeader) {
                Object[] fieldNames = new Object[args.length];
                fieldNames[0] = quoteValue(TIMESTAMP_FIELD_NAME);
                if (hasException) {
                    fieldNames[1] = quoteValue(LEVEL_FIELD_NAME);
                    fieldNames[2] = quoteValue(EXCEPTION_FIELD_NAME);
                    fieldNames[3] = quoteValue(POSITION_FIELD_NAME);
                }
                for (int i = 0; i < fieldCount; i++) {
                    fieldNames[i+prefixCount] = 
                        quoteValue(stripColumnQualifier(getName(i)));
                }
                ps.printf(format, fieldNames);
                needsHeader = false;
            }
            ps.printf(format, args);
        }

        private final char LINE_DELIM = '\n';
        private final char FIELD_DELIM = ',';
        private final char QUOTE_CHAR = '"';
        private final String ONE_QUOTE = "\"";
        private final String TWO_QUOTES = "\"\"";

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
            if (s.indexOf(LINE_DELIM) == -1
                && s.indexOf(FIELD_DELIM) == -1
                && s.charAt(0) != QUOTE_CHAR)
            {
                return s;
            }

            String replaced = s.replaceAll(ONE_QUOTE, TWO_QUOTES);
            return ONE_QUOTE + replaced + ONE_QUOTE;
        }

        /**
         * Fields of generated Java code look like: ID$0$[COLNAME]
         * This methods strips everything up to the last '$'.
         * 
         * @param fieldName name of field to be cleaned stripped
         */
        private String stripColumnQualifier(String fieldName)
        {
            int lastDollar = fieldName.lastIndexOf('$');
            if (lastDollar == -1) {
                return fieldName;
            }
            return fieldName.substring(lastDollar+1);
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
    private class SummaryLogEntry extends SyntheticObject
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
    private class SummaryLogger extends DefaultErrorLogger
    {
        private SummaryLogger(String filename)
        {
            super(filename, false);
        }

        /**
         * Logs an entry into the summary file. This method must be 
         * synchronized as various sessions may try to write into the 
         * same summary file.
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
    private class ErrorQuotaLogger extends ErrorLoggerBase
    {
        private ErrorLogger logger;
        private ErrorQuota quota;
        
        public ErrorQuotaLogger(ErrorLogger logger, ErrorQuota quota)
        {
            this.logger = logger;
            this.quota = quota;
        }

        // implement ErrorLogger
        public void log(
            RuntimeException ex, 
            int columnIndex, 
            String tag,
            boolean isWarning)
        {
            synchronized (quota) {
                if (isWarning) {
                    quota.warningCount++;
                } else {
                    quota.errorCount++;
                }
                // NOTE: if an error causes an exception, do not log it
                if (quota.errorCount > quota.errorMax) {
                    EigenbaseException ex2;
                    String field = getFieldName(tag, columnIndex);
                    String row = getRecordString();
                    String messages = Util.getMessages(ex);
                    if (columnIndex == 0) {
                        ex2 = 
                            FarragoResource.instance().JavaCalcConditionError
                            .ex(row, messages);
                    } else { 
                        ex2 = 
                            FarragoResource.instance().JavaCalcError.ex(
                                field, row, messages);
                    }
                    if (quota.errorMax > 0) {
                        throw FarragoResource.instance().ErrorLimitExceeded.ex(
                            quota.errorMax, Util.getMessages(ex2));
                    }
                    throw ex2;
                }
                // Log the error if the logging limit has not been reached
                if ((quota.errorCount+quota.warningCount) <= quota.errorLogMax)
                {
                    if (row != null) {
                        logger.log(row, ex, columnIndex, tag, isWarning);
                    } else {
                        logger.log(
                            names, values, ex, columnIndex, tag, isWarning);
                    }
                }
            }
        }

        // implement ErrorLogger
        public String getFilename()
        {
            // Return empty filename if no logging was done. No logging was 
            // done if the error limit was zero and there were no warnings,
            // or if no logging was allowed.
            if ((quota.errorMax == 0 && quota.warningCount == 0) 
                || quota.errorLogMax == 0)
            {
                return "";
            }
            return logger.getFilename();
        }
    }
}

// End LucidDbRuntimeContext.java
