/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.test.concurrent;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import junit.framework.TestCase;

import net.sf.farrago.trace.FarragoTrace;

import org.eigenbase.runtime.IteratorResultSet;
import org.eigenbase.util.Util;


/**
 * FarragoTestConcurrentScriptedCommandGenerator creates instances of
 * {@link FarragoTestConcurrentCommand} that perform specific actions in a
 * specific order and within the context of a test thread
 * ({@link FarragoTestConcurrentCommandExecutor}).
 *
 * <p>Actions are loaded from a script.
 *
 * TODO: Put script definition in package.html and link to it.
 *
 * <p>A single FarragoTestConcurrentScriptedCommandGenerator creates commands
 * for multiple threads.  Each thread is represented by an integer
 * "thread ID" and, optionally, a String thread name.  Thread IDs may
 * take on any positive integer value and may be a sparse set (e.g. 1,
 * 2, 5).  Thread names may be any String.
 *
 * <p>When each command is created, it is associated with a thread and
 * given an execution order.  Execution order values are positive
 * integers, must be unique within a thread, and may be a sparse set
 * See
 * {@link FarragoTestConcurrentTestCase#executeTest(FarragoTestConcurrentCommandGenerator,
 *                                               boolean)}
 * for other considerations.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FarragoTestConcurrentScriptedCommandGenerator
    extends FarragoTestConcurrentCommandGenerator
{
    //~ Static fields/initializers --------------------------------------------

    private static final String PRE_SETUP_STATE = "pre-setup";
    private static final String SETUP_STATE = "setup";
    private static final String POST_SETUP_STATE = "post-setup";
    private static final String THREAD_STATE = "thread";
    private static final String REPEAT_STATE = "repeat";
    private static final String SQL_STATE = "sql";
    private static final String POST_THREAD_STATE = "post-thread";
    private static final String EOF_STATE = "eof";

    private static final String LOCKSTEP = "@lockstep";
    private static final String NOLOCKSTEP = "@nolockstep";
    private static final String ENABLED = "@enabled";
    private static final String DISABLED = "@disabled";
    private static final String SETUP = "@setup";
    private static final String END = "@end";
    private static final String THREAD = "@thread";
    private static final String REPEAT = "@repeat";
    private static final String SYNC = "@sync";
    private static final String TIMEOUT = "@timeout";
    private static final String PREPARE = "@prepare";
    private static final String FETCH = "@fetch";
    private static final String CLOSE = "@close";
    private static final String SLEEP = "@sleep";
    
    private static final String SQL = "";
    private static final String EOF = null;

    private static final Object[][] STATE_TABLE = {
        { PRE_SETUP_STATE,   new Object[][] { { LOCKSTEP,   PRE_SETUP_STATE },
                                              { NOLOCKSTEP, PRE_SETUP_STATE },
                                              { ENABLED,    PRE_SETUP_STATE },
                                              { DISABLED,   PRE_SETUP_STATE },
                                              { SETUP,      SETUP_STATE },
                                              { THREAD,     THREAD_STATE } } },

        { SETUP_STATE,       new Object[][] { { END,        POST_SETUP_STATE },
                                              { SQL,        SETUP_STATE } } },

        { POST_SETUP_STATE,  new Object[][] { { THREAD,     THREAD_STATE } } },

        { THREAD_STATE,      new Object[][] { { REPEAT,     REPEAT_STATE },
                                              { SYNC,       THREAD_STATE },
                                              { TIMEOUT,    THREAD_STATE },
                                              { PREPARE,    THREAD_STATE },
                                              { FETCH,      THREAD_STATE },
                                              { CLOSE,      THREAD_STATE },
                                              { SLEEP,      THREAD_STATE },
                                              { SQL,        THREAD_STATE },
                                              { END,        POST_THREAD_STATE }
                                            } },

        { REPEAT_STATE,      new Object[][] { { SYNC,       REPEAT_STATE },
                                              { TIMEOUT,    REPEAT_STATE },
                                              { PREPARE,    REPEAT_STATE },
                                              { FETCH,      REPEAT_STATE },
                                              { CLOSE,      REPEAT_STATE },
                                              { SLEEP,      REPEAT_STATE },
                                              { SQL,        REPEAT_STATE },
                                              { END,        THREAD_STATE } } },

        { POST_THREAD_STATE, new Object[][] { { THREAD,     THREAD_STATE },
                                              { EOF,        EOF_STATE } } }
    };

    private static final int FETCH_LEN = FETCH.length();
    private static final int PREPARE_LEN = PREPARE.length();
    private static final int REPEAT_LEN = REPEAT.length();
    private static final int SLEEP_LEN = SLEEP.length();
    private static final int THREAD_LEN = THREAD.length();
    private static final int TIMEOUT_LEN = TIMEOUT.length();

    private static final char[] spaces;
    private static final char[] dashes;

    private static final int BUF_SIZE = 1024;
    private static final int REPEAT_READ_AHEAD_LIMIT = 65536;

    static {
        spaces = new char[BUF_SIZE];
        dashes = new char[BUF_SIZE];

        for(int i = 0; i < BUF_SIZE; i++) {
            spaces[i] = ' ';
            dashes[i] = '-';
        }
    }

    public static final Integer SETUP_THREAD_ID = new Integer(-1);

    //~ Instance fields -------------------------------------------------------

    private Boolean lockstep;
    
    private Boolean disabled;

    private List setupCommands = new ArrayList();

    private Map threadBufferedWriters = new HashMap();

    private Map threadStringWriters = new HashMap();

    //~ Constructors ----------------------------------------------------------

    /**
     * Constructs a new FarragoTestConcurrentScriptedCommandGenerator.
     */
    public FarragoTestConcurrentScriptedCommandGenerator(String filename)
        throws IOException
    {
        super();

        parseScript(filename);

        for(Iterator i = getThreadIds().iterator(); i.hasNext(); ) {
            Integer threadId = (Integer)i.next();

            StringWriter w = new StringWriter();
            threadStringWriters.put(threadId, w);
            threadBufferedWriters.put(threadId, new BufferedWriter(w));
        }

        StringWriter w = new StringWriter();
        threadStringWriters.put(SETUP_THREAD_ID, w);
        threadBufferedWriters.put(SETUP_THREAD_ID, new BufferedWriter(w));
    }

    //~ Methods ---------------------------------------------------------------

    boolean useLockstep()
    {
        if (lockstep == null) {
            return false;
        }

        return lockstep.booleanValue();
    }


    boolean isDisabled()
    {
        if (disabled == null) {
            return false;
        }

        return disabled.booleanValue();
    }

    void executeSetup(String jdbcUrl)
        throws Exception
    {
        if (setupCommands == null || setupCommands.size() == 0) {
            return;
        }

        Connection connection = DriverManager.getConnection(jdbcUrl);
        connection.setAutoCommit(false);

        try {
            for(Iterator i = setupCommands.iterator(); i.hasNext(); ) {
                String sql = ((String)i.next()).trim();
                
                storeSql(SETUP_THREAD_ID, sql);

                if (sql.endsWith(";")) {
                    sql = sql.substring(0, sql.length() - 1);
                }

                if (isSelect(sql)) {
                    Statement stmt = connection.createStatement();
                    try {
                        ResultSet rset = stmt.executeQuery(sql);

                        storeResults(SETUP_THREAD_ID, rset, false);
                    } finally {
                        stmt.close();
                    }
                } else if (sql.equalsIgnoreCase("commit")) {
                    connection.commit();
                } else if (sql.equalsIgnoreCase("rollback")) {
                    connection.rollback();
                } else {
                    Statement stmt = connection.createStatement();
                    try {
                        int rows = stmt.executeUpdate(sql);
                        
                        if (rows != 1) {
                            storeMessage(SETUP_THREAD_ID,
                                         String.valueOf(rows)
                                         + " rows affected.");
                        } else {
                            storeMessage(SETUP_THREAD_ID, "1 row affected.");
                        }
                    } finally {
                        stmt.close();
                    }
                }
            }
        } finally {
            connection.rollback();
            connection.close();
        }
    }

    /**
     * Parses a multi-threaded script and converts it into test
     * commands.
     */
    private void parseScript(String mtsqlFile)
        throws IOException
    {
        BufferedReader in = new BufferedReader(new FileReader(mtsqlFile));

        try {
            String state = PRE_SETUP_STATE;

            int threadId = 1;
            int nextThreadId = 1;
            int order = 1;
            int repeatCount = 0;

            while(!EOF_STATE.equals(state)) {
                String line = in.readLine();
                String trimmedLine = "";
                if (line != null) {
                    trimmedLine = line.trim();
                }

                Map commandStateMap = lookupState(state);

                String command = null;
                boolean isSql = false;

                if (trimmedLine.startsWith("@")) {
                    command = firstWord(trimmedLine);
                    if (!commandStateMap.containsKey(command)) {
                        throw new IllegalStateException(
                            "Command '" + command + "' not allowed in '"
                            + state + "' state");
                    }
                } else if (line == null) {
                    if (!commandStateMap.containsKey(EOF)) {
                        throw new IllegalStateException(
                            "Premature end of file in '" + state
                            + "' state");
                    }
                    
                    command = EOF;
                } else if (trimmedLine.equals("") ||
                           trimmedLine.startsWith("--")) {
                    continue;
                } else {
                    if (!commandStateMap.containsKey(SQL)) {
                        throw new IllegalStateException(
                            "SQL not allowed in '" + state + "' state");
                    }

                    isSql = true;
                }

                if (isSql) {
                    command = SQL;
                    
                    String sql = readSql(line, in);

                    if (SETUP_STATE.equals(state)) {
                        setupCommands.add(sql.toString().trim());
                    } else if (THREAD_STATE.equals(state) || 
                               REPEAT_STATE.equals(state)) {
                        boolean isSelect = isSelect(sql);

                        for(int i = threadId; i < nextThreadId; i++) {
                            addCommand(
                                i,
                                order,
                                (isSelect
                                 ? (AbstractCommand)new SelectCommand(sql)
                                 : (AbstractCommand)new SqlCommand(sql)));
                        }
                        order++;
                    } else {
                        assert(false);
                    }
                } else {
                    // commands are handled here
                    if (LOCKSTEP.equals(command)) {
                        assert(lockstep == null): 
                            LOCKSTEP + " and " + NOLOCKSTEP
                            + " may only appear once";
                        lockstep = Boolean.TRUE;
                    } else if (NOLOCKSTEP.equals(command)) {
                        assert(lockstep == null): 
                            LOCKSTEP + " and " + NOLOCKSTEP
                            + " may only appear once";
                        lockstep = Boolean.FALSE;
                    } else if (DISABLED.equals(command)) {
                        assert(disabled == null): 
                            DISABLED + " and " + ENABLED 
                            + " may only appear once";
                        disabled = Boolean.TRUE;
                    } else if (ENABLED.equals(command)) {
                        assert(disabled == null): 
                            DISABLED + " and " + ENABLED 
                            + " may only appear once";
                        disabled = Boolean.FALSE;
                    } else if (SETUP.equals(command)) {
                        //  nothing to do
                    } else if (THREAD.equals(command)) {
                        String threadNamesStr = 
                            trimmedLine.substring(THREAD_LEN).trim();
                        StringTokenizer threadNamesTok = 
                            new StringTokenizer(threadNamesStr, ",");

                        while(threadNamesTok.hasMoreTokens()) {
                            setThreadName(nextThreadId++,
                                          threadNamesTok.nextToken());
                        }

                        order = 1;
                    } else if (REPEAT.equals(command)) {
                        repeatCount = Integer.parseInt(
                            trimmedLine.substring(REPEAT_LEN).trim());
                        assert(repeatCount > 0): "Repeat count must be > 0";

                        in.mark(REPEAT_READ_AHEAD_LIMIT);
                    } else if (END.equals(command)) {
                        if (SETUP_STATE.equals(state)) {
                            // nothing to do
                        } else if (THREAD_STATE.equals(state)) {
                            threadId = nextThreadId;
                        } else if (REPEAT_STATE.equals(state)) {
                            repeatCount--;
                            if (repeatCount > 0) {
                                try {
                                    in.reset();
                                } catch(IOException e) {
                                    throw new IllegalStateException(
                                        "Unable to reset reader -- repeat "
                                        + "contents must be less than "
                                        + REPEAT_READ_AHEAD_LIMIT + " bytes");
                                }

                                // don't let the state change
                                continue;
                            }
                        } else {
                            assert(false);
                        }
                    } else if (SYNC.equals(command)) {
                        for(int i = threadId; i < nextThreadId; i++) {
                            addSynchronizationCommand(i, order);
                        }
                        order++;
                    } else if (TIMEOUT.equals(command)) {
                        String args = 
                            trimmedLine.substring(TIMEOUT_LEN).trim();
                        String millisStr = firstWord(args);
                        long millis = Long.parseLong(millisStr);
                        assert(millis >= 0L): "Timeout must be >= 0";

                        String sql = readSql(skipFirstWord(args).trim(), in);

                        boolean isSelect = isSelect(sql);
                        
                        for(int i = threadId; i < nextThreadId; i++) {
                            addCommand(
                                i,
                                order,
                                (isSelect
                                 ? (AbstractCommand)new SelectCommand(sql,
                                                                      millis)
                                 : (AbstractCommand)new SqlCommand(sql,
                                                                   millis)));
                        }
                        order++;
                    } else if (PREPARE.equals(command)) {
                        String startOfSql = 
                            trimmedLine.substring(PREPARE_LEN).trim();
                        
                        String sql = readSql(startOfSql, in);

                        for(int i = threadId; i < nextThreadId; i++) {
                            addCommand(i, order, new PrepareCommand(sql));
                        }
                        order++;
                    } else if (FETCH.equals(command)) {
                        String millisStr = 
                            trimmedLine.substring(FETCH_LEN).trim();

                        long millis = 0L;
                        if (millisStr.length() > 0) {
                            millis = Long.parseLong(millisStr);
                            assert(millis >= 0L): "Fetch timeout must be >= 0";
                        }

                        for(int i = threadId; i < nextThreadId; i++) {
                            addCommand(
                                i, order, new FetchAndPrintCommand(millis));
                        }
                        order++;
                    } else if (CLOSE.equals(command)) {
                        for(int i = threadId; i < nextThreadId; i++) {
                            addCloseCommand(i, order);
                        }
                        order++;
                    } else if (SLEEP.equals(command)) {
                        long millis = Long.parseLong(
                            trimmedLine.substring(SLEEP_LEN).trim());
                        assert(millis >= 0L): "Sleep timeout must be >= 0";

                        for(int i = threadId; i < nextThreadId; i++) {
                            addSleepCommand(i, order, millis);
                        }
                        order++;
                    } else {
                        assert(command == EOF): "Unknown command " + command;
                    }
                }
                
                state = (String)commandStateMap.get(command);
                assert(state != null);
            }
        } finally {
            in.close();
        }
    }

    /**
     * Converts a state name into a map.  Map keys are the names of
     * available commands (e.g. @sync), and map values are the state
     * to switch to open seeing the command.
     */
    private Map lookupState(String state)
    {
        assert(state != null);

        for(int i = 0, n = STATE_TABLE.length; i < n; i++) {
            if (state.equals(STATE_TABLE[i][0])) {
                Object[][] stateData = (Object[][])STATE_TABLE[i][1];

                HashMap result = new HashMap();
                for(int j = 0, m = stateData.length; j < m; j++) {
                    result.put(stateData[j][0], stateData[j][1]);
                }
                return result;
            }
        }

        assert(false);

        return null;
    }

    /**
     * Returns the first word of the given line, assuming the line is
     * trimmed.  Returns the characters up the first non-whitespace
     * character in the line.
     */
    private String firstWord(String trimmedLine)
    {
        return trimmedLine.replaceFirst("\\s.*", "");
    }

    /**
     * Returns everything but the first word of the given line,
     * assuming the line is trimmed.  Returns the characters following
     * the first series of consecutive whitespace characters in the
     * line.
     */
    private String skipFirstWord(String trimmedLine)
    {
        return trimmedLine.replaceFirst("^\\S+\\s+", "");
    }

    /**
     * Returns a block of SQL, starting with the given String.  Returns
     * <code>startOfSql</code> concatenated with each line from
     * <code>in</code> until a line ending with a semicolon is found.
     */
    private String readSql(String startOfSql, BufferedReader in)
        throws IOException
    {
        StringBuffer sql = new StringBuffer(startOfSql);
        sql.append('\n');

        if (!startOfSql.trim().endsWith(";")) {
            String line;
            while((line = in.readLine()) != null) {
                sql.append(line).append('\n');
                if (line.trim().endsWith(";")) {
                    break;
                }
            }
        }

        return sql.toString();
    }

    /**
     * Determines if a block of SQL is a select statment or not.
     */
    private boolean isSelect(String sql)
    {
        BufferedReader rdr = new BufferedReader(new StringReader(sql));

        try {
            String line;
            while((line = rdr.readLine()) != null) {
                line = line.trim().toLowerCase();
                if (line.startsWith("--")) {
                    continue;
                }
                
                if (line.startsWith("select") || line.startsWith("explain")) {
                    return true;
                } else {
                    return false;
                }
            }
        } catch(IOException e) {
            assert(false): "IOException via StringReader";
        } finally {
            try {
                rdr.close();
            } catch(IOException e) {
                assert(false): "IOException via StringReader";
            }
        }
            
        return false;
    }


    /**
     * Returns a map of thread ids to result data for the thread.  The
     * result data is an <code>String[2]</code> containing the thread
     * name and the thread's output.
     */
    public Map getResults()
    {
        TreeMap results = new TreeMap();

        TreeSet threadIds = new TreeSet(getThreadIds());
        threadIds.add(SETUP_THREAD_ID);

        for(Iterator i = threadIds.iterator(); i.hasNext(); ) {
            Integer threadId = (Integer)i.next();
            String threadName = getThreadName(threadId);
            try {
                BufferedWriter bout = 
                    (BufferedWriter)threadBufferedWriters.get(threadId);
                bout.flush();
            } catch(IOException e) {
                assert(false): "IOException via StringWriter";
            }
            
            StringWriter out = (StringWriter)threadStringWriters.get(threadId);

            results.put(threadId, new String[] { threadName, out.toString() });
        }

        return results;
    }


    /**
     * Causes errors to be send here for custom handling.  See
     * {@link #customErrorHandler(FarragoTestConcurrentCommandExecutor)}.
     */
    public boolean requiresCustomErrorHandling()
    {
        return true;
    }

    public void customErrorHandler(
        FarragoTestConcurrentCommandExecutor executor)
    {
        StringBuffer message = new StringBuffer();
        Throwable cause = executor.getFailureCause();
        message.append(cause.getMessage());

        StackTraceElement[] trace = cause.getStackTrace();
        for(int i = 0; i < trace.length; i++) {
            message
                .append("\n\t")
                .append(trace[i].toString());
        }

        storeMessage(executor.getThreadId(), message.toString());
    }

    /**
     * Retrieves the output stream for the given thread id.
     *
     * @return a BufferedWriter on a StringWriter for the thread.
     */
    private BufferedWriter getThreadWriter(Integer threadId)
    {
        assert(threadBufferedWriters.containsKey(threadId));

        return (BufferedWriter)threadBufferedWriters.get(threadId);
    }

    /**
     * Outputs a ResultSet for a thread.
     */
    private void storeResults(Integer threadId,
                              ResultSet rset,
                              boolean timeoutSet)
        throws SQLException
    {
        BufferedWriter out = getThreadWriter(threadId);
        int[] widths = null;

        try {
            ResultSetMetaData meta = rset.getMetaData();

            int columns = meta.getColumnCount();

            String[] values = new String[columns];
            String[] labels = new String[columns];
            widths = new int[columns];
            for(int i = 0; i < columns; i++) {
                labels[i] = meta.getColumnLabel(i + 1);
                widths[i] = Math.max(labels[i].length(),
                                     meta.getColumnDisplaySize(i + 1));
            }

            printSeparator(out, widths);
            printRow(out, labels, widths);
            printSeparator(out, widths);

            int rowCount = 0;
            while(rset.next()) {
                if (rowCount > 0 && rowCount % 100 == 0) {
                    printSeparator(out, widths);
                    printRow(out, labels, widths);
                    printSeparator(out, widths);
                }
                
                for(int i = 0; i < columns; i++) {
                    values[i] = rset.getString(i + 1);
                }

                printRow(out, values, widths);

                rowCount++;
            }
        } catch (IteratorResultSet.SqlTimeoutException e) {
            if (!timeoutSet) {
                throw e;
            }

            Util.swallow(e, FarragoTrace.getTestTracer());
        } finally {
            printSeparator(out, widths);
            try {
                out.newLine();
            } catch(IOException e) {
                assert(false): "IOException via a StringWriter";
            }

            rset.close();
        }
    }

    /**
     * Prints an output table separator.  Something like
     * <code>"+----+--------+"</code>.
     */
    private void printSeparator(BufferedWriter out, int[] widths)
    {
        try {
            for(int i = 0; i < widths.length; i++) {
                if (i > 0) {
                    out.write("-+-");
                } else {
                    out.write("+-");
                }

                int numDashes = widths[i];
                while(numDashes > 0) {
                    out.write(dashes, 0, Math.min(numDashes, BUF_SIZE));
                    numDashes -= Math.min(numDashes, BUF_SIZE);
                }
            }
            out.write("-+");
            out.newLine();
        } catch(IOException e) {
            assert(false): "IOException on StringWriter";
        }
    }

    /**
     * Prints an output table row.  Something like
     * <code>"| COL1 | COL2    |"</code>.
     */
    private void printRow(BufferedWriter out, String[] values, int[] widths)
    {
        try {
            for(int i = 0; i < values.length; i++) {
                String value = values[i];
                if (value == null) {
                    value = "";
                }

                if (i > 0) {
                    out.write(" | ");
                } else {
                    out.write("| ");
                }
                out.write(value);
                int excess = widths[i] - value.length();
                while(excess > 0) {
                    out.write(spaces, 0, Math.min(excess, BUF_SIZE));
                    excess -= Math.min(excess, BUF_SIZE);
                }
            }
            out.write(" |");
            out.newLine();
        } catch(IOException e) {
            assert(false): "IOException on StringWriter";
        }
    }


    /**
     * Prints the given SQL to the thread's output.
     */
    private void storeSql(Integer threadId, String sql)
    {
        StringBuffer message = new StringBuffer();

        BufferedReader rdr = new BufferedReader(new StringReader(sql));

        try {
            String line;
            while((line = rdr.readLine()) != null) {
                line = line.trim();

                if (message.length() > 0) {
                    message.append('\n');
                }

                message.append("> ").append(line);
            }
        } catch(IOException e) {
            assert(false): "IOException via StringReader";
        } finally {
            try {
                rdr.close();
            } catch(IOException e) {
                assert(false): "IOException via StringReader";
            }
        }
            
        storeMessage(threadId, message.toString());
    }


    /**
     * Prints the given message to the thread's output.
     */
    private void storeMessage(Integer threadId, String message)
    {
        BufferedWriter out = getThreadWriter(threadId);

        try {
            out.write(message);
            out.newLine();
        } catch(IOException e) {
            assert(false): "IOException on StringWriter";
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    private static abstract class CommandWithTimeout
        extends AbstractCommand
    {
        private long timeout;

        private CommandWithTimeout(long timeout)
        {
            this.timeout = timeout;
        }

        protected boolean setTimeout(Statement stmt)
            throws SQLException
        {
            assert (timeout >= 0);

            if (timeout > 0) {
                // TODO: add support for millisecond timeouts to
                // FarragoJdbcEngineStatement
                assert(timeout >= 1000);
                stmt.setQueryTimeout((int)(timeout / 1000));
                return true;
            }

            return false;
        }
    }

    /**
     * SelectCommand creates and executes a SQL select statement, with
     * optional timeout.
     */
    private class SelectCommand extends CommandWithTimeout
    {
        private String sql;
        private long timeout;

        private SelectCommand(String sql)
        {
            super(0);

            this.sql = sql;
        }

        private SelectCommand(String sql, long timeout)
        {
            super(timeout);

            this.sql = sql;
        }

        protected void doExecute(FarragoTestConcurrentCommandExecutor executor)
            throws SQLException
        {
            String properSql = sql.trim();

            storeSql(executor.getThreadId(), properSql);

            if (properSql.endsWith(";")) {
                properSql = properSql.substring(0, properSql.length() - 1);
            }

            PreparedStatement stmt =
                executor.getConnection().prepareStatement(properSql);

            boolean timeoutSet = setTimeout(stmt);

            try {
                storeResults(executor.getThreadId(),
                             stmt.executeQuery(),
                             timeoutSet);
            } finally {
                stmt.close();
            }
        }
    }


    /**
     * SelectCommand creates and executes a SQL select statement, with
     * optional timeout.
     */
    private class SqlCommand extends CommandWithTimeout
    {
        private String sql;
        private long timeout;

        private SqlCommand(String sql)
        {
            super(0);

            this.sql = sql;
        }

        private SqlCommand(String sql, long timeout)
        {
            super(timeout);

            this.sql = sql;
        }

        protected void doExecute(FarragoTestConcurrentCommandExecutor executor)
            throws SQLException
        {
            String properSql = sql.trim();

            storeSql(executor.getThreadId(), properSql);

            if (properSql.endsWith(";")) {
                properSql = properSql.substring(0, properSql.length() - 1);
            }

            if (properSql.equalsIgnoreCase("commit")) {
                executor.getConnection().commit();
                return;
            } else if (properSql.equalsIgnoreCase("rollback")) {
                executor.getConnection().rollback();
                return;
            }

            PreparedStatement stmt =
                executor.getConnection().prepareStatement(properSql);

            boolean timeoutSet = setTimeout(stmt);

            try {
                int rows = stmt.executeUpdate();

                if (rows != 1) {
                    storeMessage(executor.getThreadId(),
                                 String.valueOf(rows) + " rows affected.");
                } else {
                    storeMessage(executor.getThreadId(), "1 row affected.");
                }
            } catch (IteratorResultSet.SqlTimeoutException e) {
                if (!timeoutSet) {
                    throw e;
                }

                Util.swallow(e, FarragoTrace.getTestTracer());

                storeMessage(executor.getThreadId(), "Timeout");
            } finally {
                stmt.close();
            }
        }
    }


    /**
     * PrepareCommand creates a {@link PreparedStatement}.  Stores the
     * prepared statement in the FarragoTestConcurrentCommandExecutor.
     */
    private class PrepareCommand extends AbstractCommand
    {
        private String sql;

        private PrepareCommand(String sql)
        {
            this.sql = sql;
        }

        protected void doExecute(FarragoTestConcurrentCommandExecutor executor)
            throws SQLException
        {
            String properSql = sql.trim();

            storeSql(executor.getThreadId(), properSql);

            if (properSql.endsWith(";")) {
                properSql = properSql.substring(0, properSql.length() - 1);
            }

            PreparedStatement stmt =
                executor.getConnection().prepareStatement(properSql);

            executor.setStatement(stmt);
        }
    }

    /**
     * FetchAndPrintCommand executes a previously prepared statement
     * stored inthe FarragoTestConcurrentCommandExecutor and then outputs the
     * returned rows.
     */
    private class FetchAndPrintCommand extends CommandWithTimeout
    {
        private FetchAndPrintCommand(long timeout)
        {
            super(timeout);
        }

        protected void doExecute(FarragoTestConcurrentCommandExecutor executor)
            throws SQLException
        {
            PreparedStatement stmt =
                (PreparedStatement) executor.getStatement();

            boolean timeoutSet = setTimeout(stmt);

            storeResults(executor.getThreadId(),
                         stmt.executeQuery(),
                         timeoutSet);
        }
    }
}
