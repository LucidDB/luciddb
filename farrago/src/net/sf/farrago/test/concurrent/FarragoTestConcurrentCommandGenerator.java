/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import net.sf.farrago.trace.FarragoTrace;

import org.eigenbase.runtime.IteratorResultSet;
import org.eigenbase.util.Util;


/**
 * FarragoTestConcurrentCommandGenerator creates instances of
 * {@link FarragoTestConcurrentCommand} that perform specific actions
 * in a specific order and within the context of a test thread 
 * ({@link FarragoTestConcurrentCommandExecutor}).
 *
 * <p>Typical actions include preparing a SQL statement for execution,
 * executing the statement and verifying its result set, and closing
 * the statement.
 *
 * <p>A single FarragoTestConcurrentCommandGenerator creates commands for
 * multiple threads.  Each thread is represented by an integer "thread
 * ID".  Thread IDs may take on any positive integer value and may be
 * a sparse set (e.g. 1, 2, 5).
 *
 * <p>When each command is created, it is associated with a thread and
 * given an execution order.  Execution order values are positive
 * integers, must be unique within a thread, and may be a sparse set
 * See
 * {@link FarragoTestConcurrentTestCase#executeTest(FarragoTestConcurrentCommandGenerator,
 *                                               boolean)}
 * for other considerations.
 *
 * <p>There are no restrictions on the order of command creation.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FarragoTestConcurrentCommandGenerator
{
    //~ Static fields/initializers --------------------------------------------

    private static final char APOS = '\'';
    private static final char COMMA = ',';
    private static final char LEFT_BRACKET = '{';
    private static final char RIGHT_BRACKET = '}';

    //~ Instance fields -------------------------------------------------------

    /**
     * Maps Integer thread IDs to a TreeMap.  The TreeMap vaules map
     * an Integer execution order to a {@link FarragoTestConcurrentCommand}.
     */
    private TreeMap threadMap;

    /**
     * Maps Integer thread IDs to thread names.
     */
    private TreeMap threadNameMap;

    //~ Constructors ----------------------------------------------------------

    /**
     * Constructs a new FarragoTestConcurrentCommandGenerator.
     */
    public FarragoTestConcurrentCommandGenerator()
    {
        threadMap = new TreeMap();
        threadNameMap = new TreeMap();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Adds a synchronization commands.  When a thread reaches a
     * synchronization command it stops and waits for all other
     * threads to reach their synchronization commands.  When all
     * threads have reached their synchronization commands, they are
     * all released simultaneously (or as close as one can get with
     * {@link Object#notifyAll()}).  Each thread must have exactly the
     * same number of synchronization commands.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @return the newly-added command
     */
    public FarragoTestConcurrentCommand addSynchronizationCommand(
        int threadId,
        int order)
    {
        return addCommand(
            threadId,
            order,
            new SynchronizationCommand());
    }

    /**
     * Causes the given thread to sleep for the indicated number of
     * milliseconds.  Thread executes {@link Thread#sleep(long)}.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @param millis the length of time to sleep in milliseconds
     *               (must not be negative)
     * @return the newly-added command
     */
    public FarragoTestConcurrentCommand addSleepCommand(
        int threadId,
        int order,
        long millis)
    {
        return addCommand(
            threadId,
            order,
            new SleepCommand(millis));
    }

    /**
     * Adds an "explain plan" command.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @param sql the explain plan SQL (e.g. <code>"explain plan for
     *            select * from t"</code>)
     * @return the newly-added command
     */
    public FarragoTestConcurrentCommand addExplainCommand(
        int threadId,
        int order,
        String sql)
    {
        assert (sql != null);

        FarragoTestConcurrentCommand command = new ExplainCommand(sql);

        return addCommand(threadId, order, command);
    }

    /**
     * Creates a {@link PreparedStatement} for the given SQL.  This
     * command does not execute the SQL, it merely creates a
     * PreparedStatement and stores it in the
     * FarragoTestConcurrentCommandExecutor.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @param sql the SQL to prepare (e.g. <code>"select * from t"</code>)
     * @see #addFetchAndCompareCommand(int, int, int, String)
     * @return the newly-added command
     */
    public FarragoTestConcurrentCommand addPrepareCommand(
        int threadId,
        int order,
        String sql)
    {
        assert (sql != null);

        FarragoTestConcurrentCommand command = new PrepareCommand(sql);

        return addCommand(threadId, order, command);
    }

    /**
     * Executes a previously {@link #addPrepareCommand(int, int, String)
     * prepared} SQL statement and compares its {@link ResultSet} to
     * the given data.
     *
     * <p><b>Expected data format:</b>
     * <code>{ 'row1, col1 value', 'row1, col2 value', ... },
     *       { 'row2, col1 value', 'row2, col2 value', ... },
     *       ...</code>
     * <ul>
     * <li>For string data: enclose value in apostrophes, use doubled
     *     apostrophe to include an spostrophe in the value.</li>
     * <li>For integer or real data: simply use the stringified value
     *     (e.g. 123, 12.3, 0.65).  No scientific notation is allowed.</li>
     * <li>For null values, use the word <code>null</code> without quotes.</li>
     * </ul>
     * <b>Example:</b> <code>{ 'foo', 10, 3.14, null }</code>
     *
     * <p><b>Note on timeout:</b> If the previously prepared
     * statement's {@link Statement#setQueryTimeout(int)} method
     * throws an <code>UnsupportedOperationException</code> it is
     * ignored and no timeout is set.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @param timeout the query timeout, in seconds (see above)
     * @param expected the expected results (see above)
     * @return the newly-added command
     */
    public FarragoTestConcurrentCommand addFetchAndCompareCommand(
        int threadId,
        int order,
        int timeout,
        String expected)
    {
        FarragoTestConcurrentCommand command =
            new FetchAndCompareCommand(timeout, expected);

        return addCommand(threadId, order, command);
    }

    /**
     * Closes a previously {@link #addPrepareCommand(int, int, String)
     * prepared} SQL statement.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @return the newly-added command
     */
    public FarragoTestConcurrentCommand addCloseCommand(
        int threadId,
        int order)
    {
        return addCommand(
            threadId,
            order,
            new CloseCommand());
    }

    /**
     * Executes the given SQL via {@link Statement#executeUpdate(String)}.
     * May be used for update as well as insert statements.
     *
     * <p><b>Note on timeout:</b> If the previously prepared
     * statement's {@link Statement#setQueryTimeout(int)} method
     * throws an <code>UnsupportedOperationException</code> it is
     * ignored and no timeout is set.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @param timeout the query timeout, in seconds (see above)
     * @param sql the insert/update/delete SQL
     * @return the newly-added command
     */
    public FarragoTestConcurrentCommand addInsertCommand(
        int threadId,
        int order,
        int timeout,
        String sql)
    {
        FarragoTestConcurrentCommand command = new InsertCommand(timeout, sql);

        return addCommand(threadId, order, command);
    }

    /**
     * Commits pending transaction on the thread's connection.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @return the newly-added command
     */
    public FarragoTestConcurrentCommand addCommitCommand(
        int threadId,
        int order)
    {
        return addCommand(
            threadId,
            order,
            new CommitCommand());
    }

    /**
     * Rolls back pending transaction on the thread's connection.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @return the newly-added command
     */
    public FarragoTestConcurrentCommand addRollbackCommand(
        int threadId,
        int order)
    {
        return addCommand(
            threadId,
            order,
            new RollbackCommand());
    }

    /**
     * Executes a DDL statement immediately.  Assumes the statement
     * returns no information.
     * @return the newly-added command
     */
    public FarragoTestConcurrentCommand addDdlCommand(
        int threadId,
        int order,
        String ddl)
    {
        return addCommand(
            threadId,
            order,
            new DdlCommand(ddl));
    }

    /**
     * Handles adding a command to {@link #threadMap}.
     * @return the newly-added command
     */
    public FarragoTestConcurrentCommand addCommand(
        int threadId,
        int order,
        FarragoTestConcurrentCommand command)
    {
        assert (threadId > 0);
        assert (order > 0);

        Integer threadIdKey = new Integer(threadId);
        Integer orderKey = new Integer(order);

        TreeMap commandMap = (TreeMap) threadMap.get(threadIdKey);
        if (commandMap == null) {
            commandMap = new TreeMap();
            threadMap.put(threadIdKey, commandMap);
        }

        // check for duplicate order numbers
        assert (!commandMap.containsKey(orderKey));

        commandMap.put(orderKey, command);
        return command;
    }

    /**
     * Configures a human-readable name for a given thread identifier.
     * Does not imply that the thread will be created -- that only
     * happens if there are commands added to the thread.
     */
    public void setThreadName(int threadId, String name)
    {
        threadNameMap.put(new Integer(threadId), name);
    }

    /**
     * Insures that the number of commands is the same for each thread,
     * fills missing order value with null commands, and interleaves a
     * synchronization command before each actual command.  These
     * steps are required for synchronized execution in
     * FarragoConcurrencyTestCase.
     */
    void synchronizeCommandSets()
    {
        int maxCommands = 0;
        for (Iterator i = threadMap.values().iterator(); i.hasNext();) {
            TreeMap commands = (TreeMap) i.next();

            // Fill in missing slots with null (no-op) commands.
            for (int j = 1; j < ((Integer) commands.lastKey()).intValue();
                    j++) {
                Integer key = new Integer(j);
                if (!commands.containsKey(key)) {
                    commands.put(key, null);
                }
            }

            maxCommands = Math.max(
                    maxCommands,
                    commands.size());
        }

        // Make sure all threads have the same number of commands.
        for (Iterator i = threadMap.values().iterator(); i.hasNext();) {
            TreeMap commands = (TreeMap) i.next();

            if (commands.size() < maxCommands) {
                for (int j = commands.size() + 1; j <= maxCommands; j++) {
                    commands.put(
                        new Integer(j),
                        null);
                }
            }
        }

        // Interleave synchronization commands before each command.
        for (Iterator i = threadMap.entrySet().iterator(); i.hasNext();) {
            Map.Entry threadCommandsEntry = (Map.Entry) i.next();

            TreeMap commands = (TreeMap) threadCommandsEntry.getValue();

            TreeMap synchronizedCommands = new TreeMap();

            for (Iterator j = commands.entrySet().iterator(); j.hasNext();) {
                Map.Entry commandEntry = (Map.Entry) j.next();

                int orderKey = ((Integer) commandEntry.getKey()).intValue();
                FarragoTestConcurrentCommand command =
                    (FarragoTestConcurrentCommand) commandEntry.getValue();

                synchronizedCommands.put(
                    new Integer((orderKey * 2) - 1),
                    new AutoSynchronizationCommand());
                synchronizedCommands.put(
                    new Integer(orderKey * 2),
                    command);
            }

            threadCommandsEntry.setValue(synchronizedCommands);
        }
    }

    /**
     * Validates that all threads have the same number of
     * SynchronizationCommands (otherwise a deadlock is guaranteed).
     */
    void validateSynchronization(TestCase test)
    {
        int numSyncs = -1;
        for (Iterator i = threadMap.entrySet().iterator(); i.hasNext();) {
            Map.Entry threadCommandsEntry = (Map.Entry) i.next();

            TreeMap commands = (TreeMap) threadCommandsEntry.getValue();

            int numSyncsThisThread = 0;
            for (Iterator j = commands.values().iterator(); j.hasNext();) {
                if (j.next() instanceof SynchronizationCommand) {
                    numSyncsThisThread++;
                }
            }

            if (numSyncs < 0) {
                numSyncs = numSyncsThisThread;
            } else {
                test.assertEquals(
                    "mismatched synchronization command count (thread "
                    + getThreadName((Integer)threadCommandsEntry.getKey())
                    + ")", numSyncs, numSyncsThisThread);
            }
        }
    }

    /**
     * Returns a set of thread IDs.
     */
    protected Set getThreadIds()
    {
        return threadMap.keySet();
    }

    /**
     * Retrieves the name of a given thread.  If no thread names were
     * configured, returns the concatenation of "#" and the thread's
     * numeric identifier.
     *
     * @return human-readable thread name
     */
    protected String getThreadName(Integer threadId)
    {
        if (threadNameMap.containsKey(threadId)) {
            return (String)threadNameMap.get(threadId);
        } else {
            return "#" + threadId;
        }
    }

    /**
     * Indicates whether commands generated by this generator require
     * special handling.  Default implement returns false.
     */
    public boolean requiresCustomErrorHandling()
    {
        return false;
    }


    /**
     * Custom error handling occurs here if
     * {@link #requiresCustomErrorHandling()} returns true.  Default
     * implementation does nothing.
     */
    public void customErrorHandler(
        FarragoTestConcurrentCommandExecutor executor)
    {
    }


    /**
     * Returns a {@link Collection} of {@link FarragoTestConcurrentCommand}
     * objects for the given thread ID.
     */
    Collection getCommands(Integer threadId)
    {
        assert (threadMap.containsKey(threadId));

        return ((TreeMap) threadMap.get(threadId)).values();
    }

    /**
     * Returns an {@link Iterator} of {@link FarragoTestConcurrentCommand}
     * objects for the given thread ID.
     */
    Iterator getCommandIterator(Integer threadId)
    {
        return getCommands(threadId).iterator();
    }

    /**
     * Prints a description of the commands to be executed for a given
     * thread.
     */
    void printCommands(
        PrintStream out,
        Integer threadId)
    {
        int stepNumber = 1;
        for (Iterator i = getCommandIterator(threadId); i.hasNext();) {
            out.println("\tStep " + stepNumber++ + ": "
                + i.next().getClass().getName());
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    /** abstract base to handle SQLExceptions */
    protected static abstract class AbstractCommand
        implements FarragoTestConcurrentCommand
    {
        private boolean shouldFail = false;
        private String failComment = null; // describes an expected error
        private Pattern failPattern = null; // an expected error message
        private boolean failureExpected = false; // failure expected, no pattern
        
        // implement FarragoTestConcurrentCommand
        public FarragoTestConcurrentCommand markToFail(
            String comment,
            String pattern)
        {
            shouldFail = true;
            failComment = comment;
            failPattern = Pattern.compile(pattern);
            return this;
        }

        public boolean isFailureExpected() {
            return failureExpected;
        }
        
        public FarragoTestConcurrentCommand markToFail() {
            this.failureExpected = true;
            return this;
        }
        
        // subclasses define this to execute themselves
        protected abstract void doExecute(
            FarragoTestConcurrentCommandExecutor exec) throws Exception;

        // implement FarragoTestConcurrentCommand
        public void execute(FarragoTestConcurrentCommandExecutor exec)
            throws Exception
        {
            try {
                doExecute(exec);
                if (shouldFail) {
                    throw new FarragoTestConcurrentCommand.ShouldHaveFailedException(failComment);
                }
            } catch (SQLException err) {
                if (!shouldFail) {
                    throw err;
                }
                boolean matches = false;
                if (failPattern == null) {
                    matches = true; // by default
                } else {
                    for (SQLException err2 = err; err2 != null;
                            err2 = err2.getNextException()) {
                        String msg = err2.getMessage();
                        if (msg != null) {
                            matches = failPattern.matcher(msg).find();
                        }
                        if (matches) {
                            break;
                        }
                    }
                }
                if (!matches) {
                    // an unexpected error
                    throw err;
                } else {
                    // else swallow it
                    Util.swallow(
                        err,
                        FarragoTrace.getTestTracer());
                }
            }
        }
    }

    /**
     * SynchronizationCommand causes the execution thread to wait for all
     * other threads in the test before continuing.
     */
    static class SynchronizationCommand extends AbstractCommand
    {
        private SynchronizationCommand()
        {
        }

        protected void doExecute(FarragoTestConcurrentCommandExecutor executor)
            throws Exception
        {
            executor.getSynchronizer().waitForOthers();
        }
    }

    /**
     * AutoSynchronizationCommand is idential to
     * SynchronizationCommand, except that it is generated
     * automatically by the test harness and is not counted when
     * displaying the step number in which an error occurred.
     */
    static class AutoSynchronizationCommand extends SynchronizationCommand
    {
        private AutoSynchronizationCommand()
        {
            super();
        }
    }

    /**
     * SleepCommand causes the execution thread to wait for all
     * other threads in the test before continuing.
     */
    private static class SleepCommand extends AbstractCommand
    {
        private long millis;

        private SleepCommand(long millis)
        {
            this.millis = millis;
        }

        protected void doExecute(FarragoTestConcurrentCommandExecutor executor)
            throws Exception
        {
            Thread.sleep(millis);
        }
    }

    /**
     * ExplainCommand executes explain plan commands.  Automatically
     * closes the {@link Statement} before returning from
     * {@link #execute(FarragoTestConcurrentCommandExecutor)}.
     */
    private static class ExplainCommand extends AbstractCommand
    {
        private String sql;

        private ExplainCommand(String sql)
        {
            this.sql = sql;
        }

        protected void doExecute(FarragoTestConcurrentCommandExecutor executor)
            throws SQLException
        {
            Statement stmt = executor.getConnection().createStatement();

            try {
                ResultSet rset = stmt.executeQuery(sql);

                try {
                    int rowCount = 0;
                    while (rset.next()) {
                        // REVIEW: SZ 6/17/2004: Should we attempt to
                        // validate the results of the explain plan?
                        rowCount++;
                    }

                    assert (rowCount > 0);
                } finally {
                    rset.close();
                }
            } finally {
                stmt.close();
            }
        }
    }

    /**
     * PrepareCommand creates a {@link PreparedStatement}.  Stores the
     * prepared statement in the FarragoTestConcurrentCommandExecutor.
     */
    private static class PrepareCommand extends AbstractCommand
    {
        private String sql;

        private PrepareCommand(String sql)
        {
            this.sql = sql;
        }

        protected void doExecute(FarragoTestConcurrentCommandExecutor executor)
            throws SQLException
        {
            PreparedStatement stmt =
                executor.getConnection().prepareStatement(sql);

            executor.setStatement(stmt);
        }
    }

    /**
     * CloseCommand closes a previously prepared statement.  If no
     * statement is stored in the FarragoTestConcurrentCommandExecutor, it does
     * nothing.
     */
    private static class CloseCommand extends AbstractCommand
    {
        protected void doExecute(FarragoTestConcurrentCommandExecutor executor)
            throws SQLException
        {
            Statement stmt = executor.getStatement();

            if (stmt != null) {
                stmt.close();
            }

            executor.clearStatement();
        }
    }

    private static abstract class CommandWithTimeout extends AbstractCommand
    {
        private int timeout;

        private CommandWithTimeout(int timeout)
        {
            this.timeout = timeout;
        }

        protected boolean setTimeout(Statement stmt)
            throws SQLException
        {
            assert (timeout >= 0);

            if (timeout > 0) {
                stmt.setQueryTimeout(timeout);
                return true;
            }

            return false;
        }
    }

    /**
     * FetchAndCompareCommand executes a previously prepared statement
     * stored in the FarragoTestConcurrentCommandExecutor and then
     * validates the returned rows against expected data.
     */
    private static class FetchAndCompareCommand extends CommandWithTimeout
    {
        private ArrayList expected;
        private ArrayList result;

        private FetchAndCompareCommand(
            int timeout,
            String expected)
        {
            super(timeout);

            parseExpected(expected.trim());
        }

        protected void doExecute(FarragoTestConcurrentCommandExecutor executor)
            throws SQLException
        {
            PreparedStatement stmt =
                (PreparedStatement) executor.getStatement();

            boolean timeoutSet = setTimeout(stmt);

            ResultSet rset = stmt.executeQuery();

            ArrayList rows = new ArrayList();
            try {
                int rsetColumnCount = rset.getMetaData().getColumnCount();

                while (rset.next()) {
                    ArrayList row = new ArrayList();

                    for (int i = 1; i <= rsetColumnCount; i++) {
                        Object value = rset.getObject(i);
                        if (rset.wasNull()) {
                            value = null;
                        }

                        row.add(value);
                    }

                    rows.add(row);
                }
            } catch (IteratorResultSet.SqlTimeoutException e) {
                if (!timeoutSet) {
                    throw e;
                }

                Util.swallow(
                    e,
                    FarragoTrace.getTestTracer());
            } finally {
                rset.close();
            }

            result = rows;

            testValues();
        }

        /**
         * Parses expected values.  See {@link
         * FarragoTestConcurrentCommandGenerator#addFetchAndCompareCommand(int,
         * int, int, String)} for details on format of
         * <code>expected</code>.
         *
         * @throws IllegalStateException if there are formatting
         *                               errors in <code>expected</code>
         */
        private void parseExpected(String expected)
        {
            final int STATE_ROW_START = 0;
            final int STATE_VALUE_START = 1;
            final int STATE_STRING_VALUE = 2;
            final int STATE_OTHER_VALUE = 3;
            final int STATE_VALUE_END = 4;

            ArrayList rows = new ArrayList();
            int state = STATE_ROW_START;
            ArrayList row = null;
            StringBuffer value = new StringBuffer();

            for (int i = 0; i < expected.length(); i++) {
                char ch = expected.charAt(i);
                char nextCh =
                    (((i + 1) < expected.length()) ? expected.charAt(i + 1) : 0);
                switch (state) {
                case STATE_ROW_START: // find start of row
                    if (ch == LEFT_BRACKET) {
                        row = new ArrayList();
                        state = STATE_VALUE_START;
                    }
                    break;
                case STATE_VALUE_START: // start value
                    if (!Character.isWhitespace(ch)) {
                        value.setLength(0);
                        if (ch == APOS) {
                            // a string value
                            state = STATE_STRING_VALUE;
                        } else {
                            // some other kind of value
                            value.append(ch);
                            state = STATE_OTHER_VALUE;
                        }
                    }
                    break;
                case STATE_STRING_VALUE: // handle string values
                    if (ch == APOS) {
                        if (nextCh == APOS) {
                            value.append(APOS);
                            i++;
                        } else {
                            row.add(value.toString());
                            state = STATE_VALUE_END;
                        }
                    } else {
                        value.append(ch);
                    }
                    break;
                case STATE_OTHER_VALUE: // handle other values (numeric, null)
                    if ((ch != COMMA) && (ch != RIGHT_BRACKET)) {
                        value.append(ch);
                        break;
                    }
                    String stringValue = value.toString().trim();
                    if (stringValue.matches("^-?[0-9]+$")) {
                        row.add(new BigInteger(stringValue));
                    } else if (stringValue.matches("^-?[0-9]*\\.[0-9]+$")) {
                        row.add(new BigDecimal(stringValue));
                    } else if (stringValue.equals("true")) {
                        row.add(Boolean.TRUE);
                    } else if (stringValue.equals("false")) {
                        row.add(Boolean.FALSE);
                    } else if (stringValue.equals("null")) {
                        row.add(null);
                    } else {
                        throw new IllegalStateException("unknown value type '"
                            + stringValue + "' for FetchAndCompare command");
                    }

                    state = STATE_VALUE_END;

                // FALL THROUGH
                case STATE_VALUE_END: // find comma or end of row
                    if (ch == COMMA) {
                        state = STATE_VALUE_START;
                    } else if (ch == RIGHT_BRACKET) {
                        // end of row
                        rows.add(row);
                        state = STATE_ROW_START;
                    } else if (!Character.isWhitespace(ch)) {
                        throw new IllegalStateException(
                            "unexpected character '" + ch + "' at position "
                            + i + " of expected values");
                    }
                    break;
                }
            }

            if (state != STATE_ROW_START) {
                throw new IllegalStateException(
                    "unterminated data in expected values");
            }

            if (rows.size() > 1) {
                Iterator rowIter = rows.iterator();

                int expectedNumColumns = ((ArrayList) rowIter.next()).size();

                while (rowIter.hasNext()) {
                    int numColumns = ((ArrayList) rowIter.next()).size();

                    if (numColumns != expectedNumColumns) {
                        throw new IllegalStateException(
                            "all rows in expected values must have the same number of columns");
                    }
                }
            }

            this.expected = rows;
        }

        /**
         * Validates expected data against retrieved data.
         */
        private void testValues()
        {
            if (expected.size() != result.size()) {
                dumpData("Expected " + expected.size() + " rows, got "
                    + result.size());
            }

            Iterator expectedIter = expected.iterator();
            Iterator resultIter = result.iterator();

            int rowNum = 1;
            while (expectedIter.hasNext() && resultIter.hasNext()) {
                ArrayList expectedRow = (ArrayList) expectedIter.next();
                ArrayList resultRow = (ArrayList) resultIter.next();

                testValues(expectedRow, resultRow, rowNum++);
            }
        }

        /**
         * Validates {@link ResultSet} against expected data.
         */
        private void testValues(
            ArrayList expectedRow,
            ArrayList resultRow,
            int rowNum)
        {
            if (expectedRow.size() != resultRow.size()) {
                dumpData("Row " + rowNum + " Expected " + expected.size()
                    + " columns, got " + result.size());
            }

            Iterator expectedIter = expectedRow.iterator();
            Iterator resultIter = resultRow.iterator();

            int colNum = 1;
            while (expectedIter.hasNext() && resultIter.hasNext()) {
                Object expectedValue = expectedIter.next();
                Object resultValue = resultIter.next();

                if ((expectedValue == null) || expectedValue instanceof String
                        || expectedValue instanceof Boolean) {
                    test(expectedValue, resultValue, rowNum, colNum);
                } else if (expectedValue instanceof BigInteger) {
                    BigInteger expectedInt = (BigInteger) expectedValue;

                    if (expectedInt.bitLength() <= 31) {
                        test(
                            expectedInt.intValue(),
                            ((Number) resultValue).intValue(),
                            rowNum,
                            colNum);
                    } else if (expectedInt.bitLength() <= 63) {
                        test(
                            expectedInt.longValue(),
                            ((Number) resultValue).longValue(),
                            rowNum,
                            colNum);
                    } else {
                        // REVIEW: how do we return very
                        // large integer values?
                        test(expectedInt, resultValue, rowNum, colNum);
                    }
                } else if (expectedValue instanceof BigDecimal) {
                    BigDecimal expectedReal = (BigDecimal) expectedValue;

                    float asFloat = expectedReal.floatValue();
                    double asDouble = expectedReal.doubleValue();

                    if ((asFloat != Float.POSITIVE_INFINITY)
                            && (asFloat != Float.NEGATIVE_INFINITY)) {
                        test(
                            asFloat,
                            ((Number) resultValue).floatValue(),
                            rowNum,
                            colNum);
                    } else if ((asDouble != Double.POSITIVE_INFINITY)
                            && (asDouble != Double.NEGATIVE_INFINITY)) {
                        test(
                            asDouble,
                            ((Number) resultValue).doubleValue(),
                            rowNum,
                            colNum);
                    } else {
                        // REVIEW: how do we return very large decimal
                        // values?
                        test(expectedReal, resultValue, rowNum, colNum);
                    }
                } else {
                    throw new IllegalStateException(
                        "unknown type of expected value: "
                        + expectedValue.getClass().getName());
                }

                colNum++;
            }
        }

        private void test(
            Object expected,
            Object got,
            int rowNum,
            int colNum)
        {
            if ((expected == null) && (got == null)) {
                return;
            }

            if ((expected == null) || !expected.equals(got)) {
                reportError(
                    String.valueOf(expected),
                    String.valueOf(got),
                    rowNum,
                    colNum);
            }
        }

        private void test(
            int expected,
            int got,
            int rowNum,
            int colNum)
        {
            if (expected != got) {
                reportError(
                    String.valueOf(expected),
                    String.valueOf(got),
                    rowNum,
                    colNum);
            }
        }

        private void test(
            long expected,
            long got,
            int rowNum,
            int colNum)
        {
            if (expected != got) {
                reportError(
                    String.valueOf(expected),
                    String.valueOf(got),
                    rowNum,
                    colNum);
            }
        }

        private void test(
            float expected,
            float got,
            int rowNum,
            int colNum)
        {
            if (expected != got) {
                reportError(
                    String.valueOf(expected),
                    String.valueOf(got),
                    rowNum,
                    colNum);
            }
        }

        private void test(
            double expected,
            double got,
            int rowNum,
            int colNum)
        {
            if (expected != got) {
                reportError(
                    String.valueOf(expected),
                    String.valueOf(got),
                    rowNum,
                    colNum);
            }
        }

        private void reportError(
            String expected,
            String got,
            int rowNum,
            int colNum)
        {
            dumpData("Row " + rowNum + ", column " + colNum + ": expected <"
                + expected + ">, got <" + got + ">");
        }

        /**
         * Outputs expected and result data in tabular format.
         */
        private void dumpData(String message)
        {
            Iterator expectedIter = expected.iterator();
            Iterator resultIter = result.iterator();

            StringBuffer fullMessage = new StringBuffer(message);

            int rowNum = 1;
            while (expectedIter.hasNext() || resultIter.hasNext()) {
                StringBuffer expectedOut = new StringBuffer();
                expectedOut.append("Row ").append(rowNum).append(" exp:");

                StringBuffer resultOut = new StringBuffer();
                resultOut.append("Row ").append(rowNum).append(" got:");

                Iterator expectedRowIter = null;
                if (expectedIter.hasNext()) {
                    ArrayList expectedRow = (ArrayList) expectedIter.next();
                    expectedRowIter = expectedRow.iterator();
                }

                Iterator resultRowIter = null;
                if (resultIter.hasNext()) {
                    ArrayList resultRow = (ArrayList) resultIter.next();
                    resultRowIter = resultRow.iterator();
                }

                while (((expectedRowIter != null) && expectedRowIter.hasNext())
                        || ((resultRowIter != null) && resultRowIter.hasNext())) {
                    Object expectedObject =
                        ((expectedRowIter != null) ? expectedRowIter.next() : "");

                    Object resultObject =
                        ((resultRowIter != null) ? resultRowIter.next() : "");

                    String expectedValue;
                    if (expectedObject == null) {
                        expectedValue = "<null>";
                    } else {
                        expectedValue = expectedObject.toString();
                    }

                    String resultValue;
                    if (resultObject == null) {
                        resultValue = "<null>";
                    } else {
                        resultValue = resultObject.toString();
                    }

                    int width =
                        Math.max(
                            expectedValue.length(),
                            resultValue.length());

                    expectedOut.append(" | ").append(expectedValue);
                    for (int i = 0; i < (width - expectedValue.length());
                            i++) {
                        expectedOut.append(' ');
                    }

                    resultOut.append(" | ").append(resultValue);
                    for (int i = 0; i < (width - resultValue.length()); i++) {
                        resultOut.append(' ');
                    }
                }

                if ((expectedRowIter == null) && (resultRowIter == null)) {
                    expectedOut.append('|');
                    resultOut.append('|');
                }

                expectedOut.append(" |");
                resultOut.append(" |");

                fullMessage.append('\n').append(expectedOut.toString())
                    .append('\n').append(resultOut.toString());

                rowNum++;
            }

            throw new RuntimeException(fullMessage.toString());
        }
    }

    /**
     * InsertCommand exeutes an insert, update or delete SQL
     * statement.  Uses {@link Statement#executeUpdate(String)}.
     */
    private static class InsertCommand extends CommandWithTimeout
    {
        private String sql;

        private InsertCommand(
            int timeout,
            String sql)
        {
            super(timeout);

            this.sql = sql;
        }

        protected void doExecute(FarragoTestConcurrentCommandExecutor executor)
            throws SQLException
        {
            Statement stmt = executor.getConnection().createStatement();

            setTimeout(stmt);

            stmt.executeUpdate(sql);
        }
    }

    /**
     * CommitCommand commits pending transactions via
     * {@link Connection#commit()}.
     */
    private static class CommitCommand extends AbstractCommand
    {
        protected void doExecute(FarragoTestConcurrentCommandExecutor executor)
            throws SQLException
        {
            executor.getConnection().commit();
        }
    }

    /**
     * RollbackCommand rolls back pending transactions via
     * {@link Connection#rollback()}.
     */
    private static class RollbackCommand extends AbstractCommand
    {
        protected void doExecute(FarragoTestConcurrentCommandExecutor executor)
            throws SQLException
        {
            executor.getConnection().rollback();
        }
    }

    /**
     * DdlCommand executes DDL commands.  Automatically closes the
     * {@link Statement} before returning from
     * {@link #doExecute(FarragoTestConcurrentCommandExecutor)}.
     */
    private static class DdlCommand extends AbstractCommand
    {
        private String sql;

        private DdlCommand(String sql)
        {
            this.sql = sql;
        }

        protected void doExecute(FarragoTestConcurrentCommandExecutor executor)
            throws SQLException
        {
            Statement stmt = executor.getConnection().createStatement();

            try {
                stmt.execute(sql);
            } finally {
                stmt.close();
            }
        }
    }
}
