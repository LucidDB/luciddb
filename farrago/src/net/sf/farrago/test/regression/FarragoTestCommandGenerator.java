/*
// Farrago is a relational database management system.
// (C) Copyright 2004-2004, Disruptive Tech
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.test.regression;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

/**
 * FarragoTestCommandGenerator creates instances of {@link FarragoTestCommand}
 * that perform specific actions in a specific order and within the
 * context of a test thread ({@link FarragoTestCommandExecutor}).
 *
 * <p>Typical actions include preparing a SQL statement for execution,
 * executing the statement and verifying its result set, and closing
 * the statement.
 *
 * <p>A single FarragoTestCommandGenerator creates commands for
 * multiple threads.  Each thread is represented by an integer "thread
 * ID".  Thread IDs may take on any positive integer value and may be
 * a sparse set (e.g. 1, 2, 5).
 *
 * <p>When each command is created, it is associated with a thread and
 * given an execution order.  Execution order values are positive
 * integers, must be unique within a thread, and may be a sparse set
 * See
 * {@link FarragoConcurrencyTestCase#executeTest(FarragoTestCommandGenerator,
 *                                               boolean)}
 * for other considerations.
 *
 * <p>There are no restrictions on the order of command creation.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FarragoTestCommandGenerator
{
    /**
     * Maps Integer thread IDs to a TreeMap.  The TreeMap vaules map
     * an Integer execution order to a {@link FarragoTestCommand}.
     */
    private TreeMap threadMap;

    private static final char APOS = '\'';
    private static final char COMMA = ',';
    private static final char LEFT_BRACKET = '{';
    private static final char RIGHT_BRACKET = '}';

    /**
     * Constructs a new FarragoTestCommandGenerator.
     */
    public FarragoTestCommandGenerator()
    {
        threadMap = new TreeMap();
    }

    /**
     * Adds a synchronization commands.  When a thread reaches a
     * synchronziation command it stops and waits for all other
     * threads to reach their synchronization commands.  When all
     * threads have reached their synchronization commands, they are
     * all released simultaneously (or as close as one can get with
     * {@link Object#notifyAll()}).  Each thread must have exactly the
     * same number of synchronization commands.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     */
    public void addSynchronizationCommand(int threadId, int order)
    {
        addCommand(threadId, order, new SynchronizationCommand());
    }

    /**
     * Adds an "explain plan" command.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @param sql the explain plan SQL (e.g. <code>"explain plan for
     *            select * from t"</code>)
     */
    public void addExplainCommand(int threadId, int order, String sql)
    {
        assert(sql != null);

        FarragoTestCommand command = new ExplainCommand(sql);

        addCommand(threadId, order, command);
    }

    /**
     * Creates a {@link PreparedStatement} for the given SQL.  This
     * command does not execute the SQL, it merely creates a
     * PreparedStatement and stores it in the
     * FarragoTestCommandExecutor.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     * @param sql the SQL to prepare (e.g. <code>"select * from t"</code>)
     * @see #addFetchAndCompareCommand(int, int, int, String)
     */
    public void addPrepareCommand(int threadId, int order, String sql)
    {
        assert(sql != null);

        FarragoTestCommand command = new PrepareCommand(sql);

        addCommand(threadId, order, command);
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
     */
    public void addFetchAndCompareCommand(int threadId,
                                          int order,
                                          int timeout,
                                          String expected)
    {
        FarragoTestCommand command = new FetchAndCompareCommand(timeout,
                                                                expected);

        addCommand(threadId, order, command);
    }

    
    /**
     * Closes a previously {@link #addPrepareCommand(int, int, String)
     * prepared} SQL statement.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     */
    public void addCloseCommand(int threadId, int order)
    {
        addCommand(threadId, order, new CloseCommand());
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
     */
    public void addInsertCommand(int threadId,
                                 int order,
                                 int timeout,
                                 String sql)
    {
        FarragoTestCommand command = new InsertCommand(timeout, sql);

        addCommand(threadId, order, command);
    }


    /**
     * Commits pending transaction on the thread's connection.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     */
    public void addCommitCommand(int threadId, int order)
    {
        addCommand(threadId, order, new CommitCommand());
    }


    /**
     * Rolls back pending transaction on the thread's connection.
     *
     * @param threadId the thread that should execute this command
     * @param order the execution order
     */
    public void addRollbackCommand(int threadId, int order)
    {
        addCommand(threadId, order, new RollbackCommand());
    }


    /**
     * Executes a DDL statement immediately.  Assumes the statement
     * returns no information.
     */
    public void addDdlCommand(int threadId, int order, String ddl)
    {
        addCommand(threadId, order, new DdlCommand(ddl));
    }

    /**
     * Handle adding a command to {@link #threadMap}.
     */
    void addCommand(int threadId, int order, FarragoTestCommand command)
    {
        assert(threadId > 0);
        assert(order > 0);

        Integer threadIdKey = new Integer(threadId);
        Integer orderKey = new Integer(order);

        TreeMap commandMap = (TreeMap)threadMap.get(threadIdKey);
        if (commandMap == null) {
            commandMap = new TreeMap();
            threadMap.put(threadIdKey, commandMap);
        }

        // check for duplicate order numbers
        assert(!commandMap.containsKey(orderKey));

        commandMap.put(orderKey, command);
    }

    
    /**
     * Insure that the number of commands is the same for each thread,
     * fill missing order value with null commands, and interleave a
     * synchronization command before each actual command.  These
     * steps are required for synchronized execution in
     * FarragoConcurrencyTestCase.
     */
    void synchronizeCommandSets()
    {
        int maxCommands = 0;
        for(Iterator i = threadMap.values().iterator(); i.hasNext(); ) {
            TreeMap commands = (TreeMap)i.next();

            // Fill in missing slots with null (no-op) commands.
            for(int j = 1; j < ((Integer)commands.lastKey()).intValue(); j++) {
                Integer key = new Integer(j);
                if (!commands.containsKey(key)) {
                    commands.put(key, null);
                }
            }

            maxCommands = Math.max(maxCommands, commands.size());
        }

        // Make sure all threads have the same number of commands.
        for(Iterator i = threadMap.values().iterator(); i.hasNext(); ) {
            TreeMap commands = (TreeMap)i.next();

            if (commands.size() < maxCommands) {
                for(int j = commands.size() + 1; j <= maxCommands; j++) {
                    commands.put(new Integer(j), null);
                }
            }
        }


        // Interleave synchronization commands before each command.
        for(Iterator i = threadMap.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry threadCommandsEntry = (Map.Entry)i.next();

            TreeMap commands = (TreeMap)threadCommandsEntry.getValue();

            TreeMap synchronizedCommands = new TreeMap();
            
            for(Iterator j = commands.entrySet().iterator(); j.hasNext(); ) {
                Map.Entry commandEntry = (Map.Entry)j.next();

                int orderKey = ((Integer)commandEntry.getKey()).intValue();
                FarragoTestCommand command = 
                    (FarragoTestCommand)commandEntry.getValue();

                synchronizedCommands.put(new Integer(orderKey * 2 - 1),
                                         new AutoSynchronizationCommand());
                synchronizedCommands.put(new Integer(orderKey * 2),
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
        for(Iterator i = threadMap.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry threadCommandsEntry = (Map.Entry)i.next();

            TreeMap commands = (TreeMap)threadCommandsEntry.getValue();

            int numSyncsThisThread = 0;
            for(Iterator j = commands.values().iterator(); j.hasNext(); ) {
                if (j.next() instanceof SynchronizationCommand) {
                    numSyncsThisThread++;
                }
            }

            if (numSyncs < 0) {
                numSyncs = numSyncsThisThread;
            } else {
                test.assertEquals(
                    "mismatched synchronization command count (thread id "
                    + threadCommandsEntry.getKey()
                    + ")",
                    numSyncs,
                    numSyncsThisThread);
            }
        }
    }

    /**
     * Returns a set of thread IDs.
     */
    Set getThreadIds()
    {
        return threadMap.keySet();
    }


    /**
     * Returns a {@link Collection} of {@link FarragoTestCommand}
     * objects for the given thread ID.
     *
     * @throws NullPointerException if threadId was not configured
     *                              with any commands.
     */
    Collection getCommands(Integer threadId)
    {
        assert(threadMap.containsKey(threadId));

        return ((TreeMap)threadMap.get(threadId)).values();
    }


    /**
     * SynchronizationCommand causes the execution thread to wait for all
     * other threads in the test before continuing.
     */
    private static class SynchronizationCommand
        implements FarragoTestCommand
    {
        public void execute(FarragoTestCommandExecutor executor)
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
    static class AutoSynchronizationCommand
        extends SynchronizationCommand
    {
        private AutoSynchronizationCommand()
        {
            super();
        }
    }


    /**
     * ExplainCommand executes explain plan commands.  Automatically
     * closes the {@link Statement} before returning from
     * {@link #execute(FarragoTestCommandExecutor)}.
     */
    private static class ExplainCommand
        implements FarragoTestCommand
    {
        private String sql;

        private ExplainCommand(String sql)
        {
            this.sql = sql;
        }

        public void execute(FarragoTestCommandExecutor executor)
            throws SQLException
        {
            Statement stmt = executor.getConnection().createStatement();

            try {
                ResultSet rset = stmt.executeQuery(sql);

                try {
                    int rowCount = 0;
                    while(rset.next()) {
                        // REVIEW: SZ 6/17/2004: Should we attempt to
                        // validate the results of the explain plan?
                        rowCount++;
                    }

                    assert(rowCount > 0);
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
     * prepared statement in the FarragoTestCommandExecutor.
     */
    private static class PrepareCommand
        implements FarragoTestCommand
    {
        private String sql;

        private PrepareCommand(String sql)
        {
            this.sql = sql;
        }

        public void execute(FarragoTestCommandExecutor executor)
            throws SQLException
        {
            PreparedStatement stmt = 
                executor.getConnection().prepareStatement(sql);

            executor.setStatement(stmt);
        }
    }

    /**
     * CloseCommand closes a previously prepared statement.  If no
     * statement is stored in the FarragoTestCommandExecutor, it does
     * nothing.
     */
    private static class CloseCommand
        implements FarragoTestCommand
    {
        public void execute(FarragoTestCommandExecutor executor)
            throws SQLException
        {
            Statement stmt = executor.getStatement();

            if (stmt != null) {
                stmt.close();
            }

            executor.clearStatement();
        }
    }


    private static abstract class CommandWithTimeout
        implements FarragoTestCommand
    {
        private int timeout;

        private CommandWithTimeout(int timeout)
        {
            this.timeout = timeout;
        }

        protected void setTimeout(Statement stmt)
            throws SQLException
        {
            // REVIEW: SZ 6/17/2004: We'll need support for this
            // before long.
//            stmt.setQueryTimeout(timeout);
        }
    }


    /**
     * FetchAndCompareCommand executes a previously prepared statement
     * stored in the FarragoTestCommandExecutor and then validates the
     * returned rows against expected data.
     */
    private static class FetchAndCompareCommand
        extends CommandWithTimeout
    {
        private ArrayList expected;
        private ArrayList result;

        private FetchAndCompareCommand(int timeout, String expected)
        {
            super(timeout);

            parseExpected(expected.trim());
        }


        public void execute(FarragoTestCommandExecutor executor)
            throws SQLException
        {
            PreparedStatement stmt = 
                (PreparedStatement)executor.getStatement();

            setTimeout(stmt);

            ResultSet rset = stmt.executeQuery();

            try {
                int rsetColumnCount = rset.getMetaData().getColumnCount();

                ArrayList rows = new ArrayList();
                while(rset.next()) {
                      ArrayList row = new ArrayList();

                      for(int i = 1; i <= rsetColumnCount; i++) {
                          Object value = rset.getObject(i);
                          if (rset.wasNull()) {
                              value = null;
                          }

                          row.add(value);
                      }

                      rows.add(row);
                }

                result = rows;

                testValues();
            } finally {
                rset.close();
            }
        }


        /**
         * Parse expected values.  See {@link
         * FarragoTestCommandGenerator#addFetchAndCompareCommand(int,
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

            for(int i = 0; i < expected.length(); i++) {
                char ch = expected.charAt(i);
                char nextCh = (i + 1 < expected.length()
                               ? expected.charAt(i + 1)
                               : 0);
                switch(state) {
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
                    if (ch != COMMA && ch != RIGHT_BRACKET) {
                        value.append(ch);
                        break;
                    }

                    String stringValue = value.toString().trim();
                    if (stringValue.matches("^[0-9]+$")) {
                        row.add(new BigInteger(stringValue));
                    } else if (stringValue.matches("^[0-9]*\\.[0-9]+$")) {
                        row.add(new BigDecimal(stringValue));
                    } else if (stringValue.equals("null")) {
                        row.add(null);
                    } else {
                        throw new IllegalStateException(
                            "unknown value type '"
                            + stringValue
                            + "' for FetchAndCompare command");
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
                            "unexpected character '"
                            + ch
                            + "' at position "
                            + i
                            + " of expected values");
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
                
                int expectedNumColumns = ((ArrayList)rowIter.next()).size();

                while(rowIter.hasNext()) {
                    int numColumns = ((ArrayList)rowIter.next()).size();

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
                dumpData("Expected " + expected.size()
                         + " rows, got " + result.size());
            }

            Iterator expectedIter = expected.iterator();
            Iterator resultIter = result.iterator();

            int rowNum = 1;
            while(expectedIter.hasNext() && resultIter.hasNext()) {
                ArrayList expectedRow = (ArrayList)expectedIter.next();
                ArrayList resultRow = (ArrayList)resultIter.next();

                testValues(expectedRow, resultRow, rowNum++);
            }

        }

        /**
         * Validates {@link ResultSet} against expected data.
         */
        private void testValues(ArrayList expectedRow,
                                ArrayList resultRow,
                                int rowNum)
        {
            if (expectedRow.size() != resultRow.size()) {
                dumpData("Row " + rowNum
                         + " Expected "
                         + expected.size()
                         + " columns, got "
                         + result.size());
            }

            Iterator expectedIter = expectedRow.iterator();
            Iterator resultIter = resultRow.iterator();

            int colNum = 1;
            while(expectedIter.hasNext() && resultIter.hasNext()) {
                Object expectedValue = expectedIter.next();
                Object resultValue = resultIter.next();

                if (expectedValue == null || expectedValue instanceof String) {
                    test(expectedValue, resultValue, rowNum, colNum);
                } else if (expectedValue instanceof BigInteger) {
                    BigInteger expectedInt = (BigInteger)expectedValue;

                    if (expectedInt.bitLength() <= 31) {
                        test(expectedInt.intValue(),
                             ((Integer)resultValue).intValue(),
                             rowNum,
                             colNum);
                    } else if (expectedInt.bitLength() <= 63) {
                        test(expectedInt.longValue(),
                             ((Long)resultValue).longValue(),
                             rowNum,
                             colNum);
                    } else {
                        // REVIEW: how do we return very
                        // large integer values?
                        test(expectedInt, resultValue, rowNum, colNum);
                    }
                } else if (expectedValue instanceof BigDecimal) {
                    BigDecimal expectedReal =  (BigDecimal)expectedValue;

                    float asFloat = expectedReal.floatValue();
                    double asDouble = expectedReal.doubleValue();

                    if (asFloat != Float.POSITIVE_INFINITY &&
                        asFloat != Float.NEGATIVE_INFINITY) {
                        test(asFloat,
                             ((Float)resultValue).floatValue(),
                             rowNum,
                             colNum);
                    } else if (asDouble != Double.POSITIVE_INFINITY &&
                               asDouble != Double.NEGATIVE_INFINITY) {
                        test(asDouble,
                             ((Double)resultValue).doubleValue(),
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

        
        private void test(Object expected, Object got, int rowNum, int colNum)
        {
            if (expected == null && got == null) {
                return;
            }

            if (expected == null || !expected.equals(got)) {
                reportError(String.valueOf(expected),
                            String.valueOf(got),
                            rowNum,
                            colNum);
            }
        }

        private void test(int expected, int got, int rowNum, int colNum)
        {
            if (expected != got) {
                reportError(String.valueOf(expected),
                            String.valueOf(got),
                            rowNum,
                            colNum);
            }
        }

        private void test(long expected, long got, int rowNum, int colNum)
        {
            if (expected != got) {
                reportError(String.valueOf(expected),
                            String.valueOf(got),
                            rowNum,
                            colNum);
            }
        }
        
        private void test(float expected, float got, int rowNum, int colNum)
        {
            if (expected != got) {
                reportError(String.valueOf(expected),
                            String.valueOf(got),
                            rowNum,
                            colNum);
            }
        }

        private void test(double expected, double got, int rowNum, int colNum)
        {
            if (expected != got) {
                reportError(String.valueOf(expected),
                            String.valueOf(got),
                            rowNum,
                            colNum);
            }
        }
        
        private void reportError(String expected,
                                 String got,
                                 int rowNum,
                                 int colNum)
        {
            dumpData("Row " + rowNum + ", column " + colNum
                     + ": expected <" + expected
                     + ">, got <" + got + ">");
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
            while(expectedIter.hasNext() || resultIter.hasNext()) {
                StringBuffer expectedOut = new StringBuffer();
                expectedOut.append("Row ").append(rowNum).append(" exp:");

                StringBuffer resultOut = new StringBuffer();
                resultOut.append("Row ").append(rowNum).append(" got:");

                Iterator expectedRowIter = null;
                if (expectedIter.hasNext()) {
                    ArrayList expectedRow = (ArrayList)expectedIter.next();
                    expectedRowIter = expectedRow.iterator();
                }

                Iterator resultRowIter = null;
                if (resultIter.hasNext()) {
                    ArrayList resultRow = (ArrayList)resultIter.next();
                    resultRowIter = resultRow.iterator();
                }

                while(expectedRowIter != null && expectedRowIter.hasNext() &&
                      resultRowIter != null && resultRowIter.hasNext()) {
                    Object expectedObject = (expectedRowIter != null 
                                             ? expectedRowIter.next()
                                             : "");
                    
                    Object resultObject = (resultRowIter != null 
                                           ? resultRowIter.next()
                                           : "");

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

                    int width = Math.max(expectedValue.length(),
                                         resultValue.length());

                    expectedOut.append(" | ").append(expectedValue);
                    for(int i = 0; i < width - expectedValue.length(); i++) {
                        expectedOut.append(' ');
                    }

                    resultOut.append(" | ").append(resultValue);
                    for(int i = 0; i < width - resultValue.length(); i++) {
                        resultOut.append(' ');
                    }
                }

                if (expectedRowIter == null) {
                    expectedOut.append('|');
                }

                if (resultRowIter == null) {
                    resultOut.append('|');
                }

                expectedOut.append(" |");
                resultOut.append(" |");

                fullMessage
                    .append('\n')
                    .append(expectedOut.toString())
                    .append('\n')
                    .append(resultOut.toString());

                rowNum++;
            }

            throw new RuntimeException(fullMessage.toString());
        }
    }


    /**
     * InsertCommand exeutes an insert, update or delete SQL
     * statement.  Uses {@link Statement#executeUpdate(String)}.
     */
    private static class InsertCommand
        extends CommandWithTimeout
    {
        private String sql;

        private InsertCommand(int timeout, String sql)
        {
            super(timeout);

            this.sql = sql;
        }

        public void execute(FarragoTestCommandExecutor executor)
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
    private static class CommitCommand
        implements FarragoTestCommand
    {
        public void execute(FarragoTestCommandExecutor executor)
            throws SQLException
        {
            executor.getConnection().commit();
        }
    }


    /**
     * RollbackCommand rolls back pending transactions via
     * {@link Connection#rollback()}.
     */
    private static class RollbackCommand
        implements FarragoTestCommand
    {
        public void execute(FarragoTestCommandExecutor executor)
            throws SQLException
        {
            executor.getConnection().rollback();
        }
    }

    /**
     * DdlCommand executes DDL commands.  Automatically closes the
     * {@link Statement} before returning from
     * {@link #execute(FarragoTestCommandExecutor)}.
     */
    private static class DdlCommand
        implements FarragoTestCommand
    {
        private String sql;

        private DdlCommand(String sql)
        {
            this.sql = sql;
        }

        public void execute(FarragoTestCommandExecutor executor)
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
