/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
// Portions Copyright (C) 2006-2009 John V. Sichi
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
package org.eigenbase.test.concurrent;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.regex.*;

import org.eigenbase.runtime.AbstractIterResultSet;
import org.eigenbase.util.Util;


/**
 * ConcurrentTestCommandScript creates instances of {@link
 * ConcurrentTestCommand} that perform specific actions in a specific
 * order and within the context of a test thread ({@link
 * ConcurrentTestCommandExecutor}).
 *
 * <p>Actions are loaded from a script (see package javadoc for script format).
 *
 * <p>A single ConcurrentTestCommandScript creates commands
 * for multiple threads. Each thread is represented by an integer "thread ID"
 * and, optionally, a String thread name. Thread IDs may take on any positive
 * integer value and may be a sparse set (e.g. 1, 2, 5). Thread names may be any
 * String.
 *
 * <p>When each command is created, it is associated with a thread and given an
 * execution order. Execution order values are positive integers, must be unique
 * within a thread, and may be a sparse set.
 * See {@link ConcurrentTestCommandGenerator#synchronizeCommandSets} for other
 * considerations.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class ConcurrentTestCommandScript
    extends ConcurrentTestCommandGenerator
{
    //~ Static fields/initializers ---------------------------------------------

    private static final String PRE_SETUP_STATE = "pre-setup";
    private static final String SETUP_STATE = "setup";
    private static final String POST_SETUP_STATE = "post-setup";
    private static final String CLEANUP_STATE = "cleanup";
    private static final String POST_CLEANUP_STATE = "post-cleanup";
    private static final String THREAD_STATE = "thread";
    private static final String REPEAT_STATE = "repeat";
    private static final String SQL_STATE = "sql";
    private static final String POST_THREAD_STATE = "post-thread";
    private static final String EOF_STATE = "eof";

    private static final String VAR = "@var";
    private static final String LOCKSTEP = "@lockstep";
    private static final String NOLOCKSTEP = "@nolockstep";
    private static final String ENABLED = "@enabled";
    private static final String DISABLED = "@disabled";
    private static final String SETUP = "@setup";
    private static final String CLEANUP = "@cleanup";
    private static final String END = "@end";
    private static final String THREAD = "@thread";
    private static final String REPEAT = "@repeat";
    private static final String SYNC = "@sync";
    private static final String TIMEOUT = "@timeout";
    private static final String ROWLIMIT = "@rowlimit";
    private static final String PREPARE = "@prepare";
    private static final String PRINT = "@print";
    private static final String FETCH = "@fetch";
    private static final String CLOSE = "@close";
    private static final String SLEEP = "@sleep";
    private static final String ERR = "@err";
    private static final String ECHO = "@echo";
    private static final String INCLUDE = "@include";
    private static final String SHELL = "@shell";

    private static final String SQL = "";
    private static final String EOF = null;

    private static final StateAction [] STATE_TABLE =
    {
        new StateAction(
            PRE_SETUP_STATE,
            new StateDatum[] {
                new StateDatum(VAR, PRE_SETUP_STATE),
                new StateDatum(LOCKSTEP, PRE_SETUP_STATE),
                new StateDatum(NOLOCKSTEP, PRE_SETUP_STATE),
                new StateDatum(ENABLED, PRE_SETUP_STATE),
                new StateDatum(DISABLED, PRE_SETUP_STATE),
                new StateDatum(SETUP, SETUP_STATE),
                new StateDatum(CLEANUP, CLEANUP_STATE),
                new StateDatum(THREAD, THREAD_STATE)
            }),

        new StateAction(
            SETUP_STATE,
            new StateDatum[] {
                new StateDatum(END, POST_SETUP_STATE),
                new StateDatum(SQL, SETUP_STATE),
                new StateDatum(INCLUDE, SETUP_STATE),
            }),

        new StateAction(
            POST_SETUP_STATE,
            new StateDatum[] {
                new StateDatum(CLEANUP, CLEANUP_STATE),
                new StateDatum(THREAD, THREAD_STATE)
            }),

        new StateAction(
            CLEANUP_STATE,
            new StateDatum[] {
                new StateDatum(END, POST_CLEANUP_STATE),
                new StateDatum(SQL, CLEANUP_STATE),
                new StateDatum(INCLUDE, CLEANUP_STATE),
            }),

        new StateAction(
            POST_CLEANUP_STATE,
            new StateDatum[] {
                new StateDatum(THREAD, THREAD_STATE)
            }),

        new StateAction(
            THREAD_STATE,
            new StateDatum[] {
                new StateDatum(REPEAT, REPEAT_STATE),
                new StateDatum(SYNC, THREAD_STATE),
                new StateDatum(TIMEOUT, THREAD_STATE),
                new StateDatum(ROWLIMIT, THREAD_STATE),
                new StateDatum(PREPARE, THREAD_STATE),
                new StateDatum(PRINT, THREAD_STATE),
                new StateDatum(FETCH, THREAD_STATE),
                new StateDatum(CLOSE, THREAD_STATE),
                new StateDatum(SLEEP, THREAD_STATE),
                new StateDatum(SQL, THREAD_STATE),
                new StateDatum(ECHO, THREAD_STATE),
                new StateDatum(ERR, THREAD_STATE),
                new StateDatum(SHELL, THREAD_STATE),
                new StateDatum(END, POST_THREAD_STATE)
            }),

        new StateAction(
            REPEAT_STATE,
            new StateDatum[] {
                new StateDatum(SYNC, REPEAT_STATE),
                new StateDatum(TIMEOUT, REPEAT_STATE),
                new StateDatum(ROWLIMIT, REPEAT_STATE),
                new StateDatum(PREPARE, REPEAT_STATE),
                new StateDatum(PRINT, REPEAT_STATE),
                new StateDatum(FETCH, REPEAT_STATE),
                new StateDatum(CLOSE, REPEAT_STATE),
                new StateDatum(SLEEP, REPEAT_STATE),
                new StateDatum(SQL, REPEAT_STATE),
                new StateDatum(ECHO, THREAD_STATE),
                new StateDatum(ERR, THREAD_STATE),
                new StateDatum(SHELL, THREAD_STATE),
                new StateDatum(END, THREAD_STATE)
            }),

        new StateAction(
            POST_THREAD_STATE,
            new StateDatum[] {
                new StateDatum(THREAD, THREAD_STATE),
                new StateDatum(EOF, EOF_STATE)
            })
    };

    private static final int FETCH_LEN = FETCH.length();
    private static final int PREPARE_LEN = PREPARE.length();
    private static final int PRINT_LEN = PRINT.length();
    private static final int REPEAT_LEN = REPEAT.length();
    private static final int SLEEP_LEN = SLEEP.length();
    private static final int THREAD_LEN = THREAD.length();
    private static final int TIMEOUT_LEN = TIMEOUT.length();
    private static final int ROWLIMIT_LEN = ROWLIMIT.length();
    private static final int ERR_LEN = ERR.length();
    private static final int ECHO_LEN = ECHO.length();
    private static final int SHELL_LEN = SHELL.length();
    private static final int INCLUDE_LEN = INCLUDE.length();
    private static final int VAR_LEN = VAR.length();

    private static final char [] spaces;
    private static final char [] dashes;

    private static final int BUF_SIZE = 1024;
    private static final int REPEAT_READ_AHEAD_LIMIT = 65536;

    static {
        spaces = new char[BUF_SIZE];
        dashes = new char[BUF_SIZE];

        for (int i = 0; i < BUF_SIZE; i++) {
            spaces[i] = ' ';
            dashes[i] = '-';
        }
    }

    // Special "thread ids" for setup & cleanup sections; actually setup &
    // cleanup SQL is executed by the main thread, and neither are in the the
    // thread map.
    private static final Integer SETUP_THREAD_ID = -1;
    private static final Integer CLEANUP_THREAD_ID = -2;

    //~ Instance fields (representing a single script):

    private boolean quiet = false;
    private boolean verbose = false;
    private Boolean lockstep;
    private Boolean disabled;
    private VariableTable vars = new VariableTable();
    private File scriptDirectory;
    private long executionStartTime = 0;

    private List<String> setupCommands = new ArrayList<String>();
    private List<String> cleanupCommands = new ArrayList<String>();

    private Map<Integer, BufferedWriter> threadBufferedWriters =
        new HashMap<Integer, BufferedWriter>();
    private Map<Integer, StringWriter> threadStringWriters =
        new HashMap<Integer, StringWriter>();
    private Map<Integer, ResultsReader> threadResultsReaders =
        new HashMap<Integer, ResultsReader>();

    //~ Constructors -----------------------------------------------------------

    public ConcurrentTestCommandScript() throws IOException
    {
        super();
    }

    /**
     * Constructs and prepares a new ConcurrentTestCommandScript.
     */
    public ConcurrentTestCommandScript(String filename)
        throws IOException
    {
        this();
        prepare(filename,  null);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Gets ready to execute: loads script FILENAME applying external variable
     * BINDINGS.
     */
    private void prepare(
        String filename,
        List<String> bindings)
        throws IOException
    {
        vars = new VariableTable();
        CommandParser parser = new CommandParser();
        parser.rememberVariableRebindings(bindings);
        parser.load(filename);

        for (Integer threadId : getThreadIds()) {
            addThreadWriters(threadId);
        }
        // Backwards compatible: printed results always has a setup section, but
        // cleanup section is optional:
        setThreadName(SETUP_THREAD_ID, "setup");
        addThreadWriters(SETUP_THREAD_ID);
        if (!cleanupCommands.isEmpty()) {
            setThreadName(CLEANUP_THREAD_ID, "cleanup");
            addThreadWriters(CLEANUP_THREAD_ID);
        }
    }

    /** Executes the script */
    public void execute() throws Exception
    {
        executionStartTime = System.currentTimeMillis();
        executeSetup();
        ConcurrentTestCommandExecutor threads[] = innerExecute();
        executeCleanup();
        postExecute(threads);
    }

    private void addThreadWriters(Integer threadId)
    {
        StringWriter w = new StringWriter();
        BufferedWriter bw = new BufferedWriter(w);
        threadStringWriters.put(threadId, w);
        threadBufferedWriters.put(threadId, bw);
        threadResultsReaders.put(threadId, new ResultsReader(bw));
    }

    public void setQuiet(boolean val)
    {
        quiet = val;
    }

    public void setVerbose(boolean val)
    {
        verbose = val;
    }


    public boolean useLockstep()
    {
        if (lockstep == null) {
            return false;
        }

        return lockstep.booleanValue();
    }

    public boolean isDisabled()
    {
        if (disabled == null) {
            return false;
        }

        return disabled.booleanValue();
    }

    public void executeSetup() throws Exception
    {
        executeCommands(SETUP_THREAD_ID, setupCommands);
    }

    public void executeCleanup() throws Exception
    {
        executeCommands(CLEANUP_THREAD_ID, cleanupCommands);
    }

    protected void executeCommands(int threadID, List<String> commands)
        throws Exception
    {
        if ((commands == null) || (commands.size() == 0)) {
            return;
        }

        Connection connection = DriverManager.getConnection(jdbcURL, jdbcProps);
        if (connection.getMetaData().supportsTransactions()) {
            connection.setAutoCommit(false);
        }

        try {
            for (String command : commands) {
                String sql = (command).trim();

                storeSql(threadID, sql);

                if (sql.endsWith(";")) {
                    sql = sql.substring(0, sql.length() - 1);
                }

                if (isSelect(sql)) {
                    Statement stmt = connection.createStatement();
                    try {
                        ResultSet rset = stmt.executeQuery(sql);
                        storeResults(threadID, rset, false);
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
                            storeMessage(
                                threadID,
                                String.valueOf(rows)
                                    + " rows affected.");
                        } else {
                            storeMessage(threadID, "1 row affected.");
                        }
                    } finally {
                        stmt.close();
                    }
                }
            }
        } finally {
            if (connection.getMetaData().supportsTransactions()) {
                connection.rollback();
            }
            connection.close();
        }
    }

    private void storeResults(
        Integer threadId,
        ResultSet rset,
        boolean withTimeout)
        throws SQLException
    {
        ResultsReader r = threadResultsReaders.get(threadId);
        r.read(rset, withTimeout);
    }

    /**
     * Determines if a block of SQL is a select statment or not.
     */
    private boolean isSelect(String sql)
    {
        BufferedReader rdr = new BufferedReader(new StringReader(sql));

        try {
            String line;
            while ((line = rdr.readLine()) != null) {
                line = line.trim().toLowerCase();
                if (line.startsWith("--")) {
                    continue;
                }

                if (line.startsWith("select") ||
                    line.startsWith("values") ||
                    line.startsWith("explain")) {
                    return true;
                } else {
                    return false;
                }
            }
        } catch (IOException e) {
            assert (false) : "IOException via StringReader";
        } finally {
            try {
                rdr.close();
            } catch (IOException e) {
                assert (false) : "IOException via StringReader";
            }
        }

        return false;
    }

    /**
     * Builds a map of thread ids to result data for the thread. Each result
     * datum is an <code>String[2]</code> containing the thread name and the
     * thread's output.
     * @return the map.
     */
    private Map<Integer, String[]> collectResults()
    {
        TreeMap<Integer, String[]> results = new TreeMap<Integer, String[]>();

        // get all normal threads
        TreeSet<Integer> threadIds = new TreeSet<Integer>(getThreadIds());
        // add the "special threads"
        threadIds.add(SETUP_THREAD_ID);
        threadIds.add(CLEANUP_THREAD_ID);

        for (Integer threadId : threadIds) {
            try {
                BufferedWriter bout = threadBufferedWriters.get(threadId);
                if (bout != null) {
                    bout.flush();
                }
            } catch (IOException e) {
                assert (false) : "IOException via StringWriter";
            }
            String threadName = getFormattedThreadName(threadId);
            StringWriter out = threadStringWriters.get(threadId);
            if (out == null) {
                continue;
            }
            results.put(
                threadId,
                new String[]{threadName, out.toString()});
        }
        return results;
    }

    // solely for backwards-compatible output
    private String getFormattedThreadName(Integer id)
    {
        if (id < 0) {                   // special thread
            return getThreadName(id);
        } else {                        // normal thread
            return "thread " + getThreadName(id);
        }
    }

    public void printResults(BufferedWriter out) throws IOException
    {
        final Map<Integer,String[]> results = collectResults();
        printThreadResults(out, results.get(SETUP_THREAD_ID));
        for (Integer id : results.keySet()) {
            if (id < 0) {
                continue;               // special thread
            }
            printThreadResults(out, results.get(id)); // normal thread
        }
        printThreadResults(out, results.get(CLEANUP_THREAD_ID));
    }

    private void printThreadResults(BufferedWriter out, String[] threadResult)
        throws IOException
    {
        if (threadResult == null) {
            return;
        }
        String threadName = threadResult[0];
        out.write("-- " + threadName);
        out.newLine();
        out.write(threadResult[1]);
        out.write("-- end of " + threadName);
        out.newLine();
        out.newLine();
        out.flush();
    }

    /**
     * Causes errors to be send here for custom handling. See {@link
     * #customErrorHandler(ConcurrentTestCommandExecutor)}.
     */
    boolean requiresCustomErrorHandling()
    {
        return true;
    }

    void customErrorHandler(
        ConcurrentTestCommandExecutor executor)
    {
        StringBuilder message = new StringBuilder();
        Throwable cause = executor.getFailureCause();
        ConcurrentTestCommand command = executor.getFailureCommand();

        if ((command == null) || !command.isFailureExpected()) {
            message.append(cause.getMessage());
            StackTraceElement [] trace = cause.getStackTrace();
            for (StackTraceElement aTrace : trace) {
                message.append("\n\t").append(aTrace.toString());
            }
        } else {
            message.append(cause.getClass().getName()).append(": ").append(
                cause.getMessage());
        }

        storeMessage(
            executor.getThreadId(),
            message.toString());
    }

    /**
     * Retrieves the output stream for the given thread id.
     *
     * @return a BufferedWriter on a StringWriter for the thread.
     */
    private BufferedWriter getThreadWriter(Integer threadId)
    {
        assert (threadBufferedWriters.containsKey(threadId));
        return threadBufferedWriters.get(threadId);
    }


    /**
     * Saves a SQL command to be printed with the thread's output.
     */
    private void storeSql(Integer threadId, String sql)
    {
        StringBuilder message = new StringBuilder();

        BufferedReader rdr = new BufferedReader(new StringReader(sql));

        try {
            String line;
            while ((line = rdr.readLine()) != null) {
                line = line.trim();

                if (message.length() > 0) {
                    message.append('\n');
                }

                message.append("> ").append(line);
            }
        } catch (IOException e) {
            assert (false) : "IOException via StringReader";
        } finally {
            try {
                rdr.close();
            } catch (IOException e) {
                assert (false) : "IOException via StringReader";
            }
        }

        storeMessage(
            threadId,
            message.toString());
    }

    /**
     * Saves a message to be printed with the thread's output.
     */
    private void storeMessage(Integer threadId, String message)
    {
        BufferedWriter out = getThreadWriter(threadId);
        try {
            out.write(message);
            out.newLine();
        } catch (IOException e) {
            assert (false) : "IOException on StringWriter";
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class StateAction
    {
        final String state;
        final StateDatum [] stateData;

        StateAction(String state, StateDatum [] stateData)
        {
            this.state = state;
            this.stateData = stateData;
        }
    }

    private static class StateDatum
    {
        final String x;
        final String y;

        StateDatum(String x, String y)
        {
            this.x = x;
            this.y = y;
        }
    }


    // Inner Class: a symbol table of script variables

    private class VariableTable {
        private final Map<String, String> map;

        // matches $$, $var, ${var}
        private final Pattern symbolPattern =
            Pattern.compile("\\$((\\$)|([A-Za-z]\\w*)|\\{([A-Za-z]\\w*)\\})");

        public VariableTable() {
            map = new HashMap<String, String>();
        }

        public class Excn extends IllegalArgumentException {
            public Excn(String msg) {
                super(msg);
            }
        }

        public boolean isEmpty() {
            return map.isEmpty();
        }

        public boolean isDefined(String sym) {
            return map.containsKey(sym);
        }

        // a symbol must be explicitly defined before it can be set or read.
        public void define(String sym, String val) throws Excn
        {
            if (isDefined(sym)) {
                throw new Excn("second declaration of variable " + sym);
            }
            map.put(sym, val);
        }

        // returns null is SYM is not defined
        public String get(String sym) {
            if (isDefined(sym)) {
                return map.get(sym);
            } else {
                return null;
            }
        }

        public void set(String sym, String val) throws Excn
        {
            if (isDefined(sym)) {
                map.put(sym, val);
                return;
            }
            throw new Excn("undeclared variable " + sym);
        }

        public String expand(String in) {
            if (in.contains("$")) {
                StringBuffer out = new StringBuffer();
                Matcher matcher = symbolPattern.matcher(in);
                int lastEnd = 0;
                while (matcher.find()) {
                    int start = matcher.start();
                    int end = matcher.end();
                    String val = null;
                    if (null != matcher.group(2)) {
                        val = "$";          // matched $$
                    } else {
                        String var = matcher.group(3); // matched $var
                        if (var == null) {
                            var = matcher.group(4); // matched ${var}
                        }
                        if (map.containsKey(var)) {
                            val = map.get(var);
                            val = expand(val);
                        } else {
                            // not our var, so can't expand
                            val = matcher.group(0);
                        }
                    }
                    out.append(in.substring(lastEnd, start));
                    out.append(val);
                    lastEnd = end;
                }
                out.append(in.substring(lastEnd));
                return out.toString();
            } else {
                return in;
            }
        }
    }


    // Inner Class: the command parser

    private class CommandParser {
        final Pattern splitWords = Pattern.compile("\\s+");
        final Pattern splitBinding = Pattern.compile("=");
        final Pattern matchesVarDefn = Pattern.compile("([A-Za-z]\\w*) *=(.*)$");
        // \1 is VAR, \2 is VAL

        // parser state
        private String state;
        private int threadId;
        private int nextThreadId;
        private int order;
        private int repeatCount;
        private boolean scriptHasVars;
        private Stack<File> currentDirectory = new Stack<File>();

        private class Binding {
            public final String var;
            public final String val;
            public Binding(String var, String val) {
                this.var = var;
                this.val = val;
            }
            // @param phrase has form VAR=VAL
            public Binding(String phrase) {
                String[] parts = splitBinding.split(phrase);
                assert parts.length == 2;
                this.var = parts[0];
                this.val = parts[1];
            }
        }

        // A list of Bindings that must be applied immediately after parsing
        // last @var.
        private List<Binding> deferredBindings = new ArrayList<Binding>();

        public CommandParser() {
            state  = PRE_SETUP_STATE;
            threadId =  nextThreadId = 1;
            order = 1;
            repeatCount = 0;
            scriptHasVars = false;
            currentDirectory.push(null);
        }

        // Parses a set of VAR=VAL pairs from the command line, and saves it for
        // later application.
        public void rememberVariableRebindings(List<String> pairs)
        {
            if (pairs == null) {
                return;
            }
            for (String pair : pairs) {
                deferredBindings.add(new Binding(pair));
            }
        }

        // to call after all @var commands but before any SQL.
        private void applyVariableRebindings()
        {
            for (Binding binding : deferredBindings) {
                vars.set(binding.var, binding.val);
            }
        }

        // trace loading of a script
        private void trace(String prefix, Object message)
        {
            if (verbose && !quiet) {
                if (prefix != null) {
                    System.out.print(prefix + ": ");
                }
                System.out.println(message);
            }
        }

        private void trace(String message)
        {
            trace(null, message);
        }

        /**
         * Parses a multi-threaded script and converts it into test commands.
         */
        private void load(String scriptFileName) throws IOException {
            File scriptFile = new File(currentDirectory.peek(), scriptFileName);
            currentDirectory.push(scriptDirectory = scriptFile.getParentFile());
            BufferedReader in = new BufferedReader(new FileReader(scriptFile));
            try {
                String line;
                while ((line = in.readLine()) != null) {
                    line = line.trim();
                    Map<String, String> commandStateMap = lookupState(state);
                    String command = null;
                    boolean isSql = false;

                    if (line.startsWith("@")) {
                        command = firstWord(line);
                        if (!commandStateMap.containsKey(command)) {
                            throw new IllegalStateException(
                                "Command '" + command + "' not allowed in '"
                                + state + "' state");
                        }
                    } else if (line.equals("") || line.startsWith("--")) {
                        continue;
                    } else {
                        isSql = true;
                        if (!commandStateMap.containsKey(SQL)) {
                            throw new IllegalStateException(
                                "SQL not allowed in '" + state + "' state");
                        }
                    }

                    if (isSql) {
                        command = SQL;
                        String sql = readSql(line, in);

                        if (SETUP_STATE.equals(state)) {
                            trace("@setup", sql);
                            setupCommands.add(sql);
                        } else if (CLEANUP_STATE.equals(state)) {
                            trace("@cleanup", sql);
                            cleanupCommands.add(sql);
                        } else if (THREAD_STATE.equals(state)
                            || REPEAT_STATE.equals(state))
                        {
                            boolean isSelect = isSelect(sql);
                            trace(sql);
                            for (int i = threadId; i < nextThreadId; i++) {
                                CommandWithTimeout cmd =
                                    isSelect
                                    ? new SelectCommand(sql)
                                    : new SqlCommand(sql);
                                addCommand(i, order, cmd);
                            }
                            order++;
                        } else {
                            assert (false);
                        }

                    } else {
                        // commands are handled here
                        if (VAR.equals(command)) {
                            String args = line.substring(VAR_LEN).trim();
                            scriptHasVars = true;
                            trace("@var",  args);
                            defineVariables(args);

                        } else if (LOCKSTEP.equals(command)) {
                            assert (lockstep == null) : LOCKSTEP + " and "
                                + NOLOCKSTEP
                                + " may only appear once";
                            lockstep = Boolean.TRUE;
                            trace("lockstep");

                        } else if (NOLOCKSTEP.equals(command)) {
                            assert (lockstep == null) : LOCKSTEP + " and "
                                + NOLOCKSTEP
                                + " may only appear once";
                            lockstep = Boolean.FALSE;
                            trace("no lockstep");

                        } else if (DISABLED.equals(command)) {
                            assert (disabled == null) : DISABLED + " and " + ENABLED
                                + " may only appear once";
                            disabled = Boolean.TRUE;
                            trace("disabled");

                        } else if (ENABLED.equals(command)) {
                            assert (disabled == null) : DISABLED + " and " + ENABLED
                                + " may only appear once";
                            disabled = Boolean.FALSE;
                            trace("enabled");

                        } else if (SETUP.equals(command)) {
                            trace("@setup");

                        } else if (CLEANUP.equals(command)) {
                            trace("@cleanup");

                        } else if (INCLUDE.equals(command)) {
                            String includedFile =
                                line.substring(INCLUDE_LEN).trim();
                            trace("@include", includedFile);
                            load(includedFile);
                            trace("end @include", includedFile);

                        } else if (THREAD.equals(command)) {
                            String threadNamesStr =
                                line.substring(THREAD_LEN).trim();
                            trace("@thread", threadNamesStr);
                            StringTokenizer threadNamesTok =
                                new StringTokenizer(threadNamesStr, ",");
                            while (threadNamesTok.hasMoreTokens()) {
                                setThreadName(
                                    nextThreadId++,
                                    threadNamesTok.nextToken());
                            }

                            // Since DDL commands are prepared and executed,
                            // defer any DDL validation until execute time

                            order = 1;
                            String defer = "alter session set \"validateDdlOnPrepare\" = false";
                            for (int i = threadId; i < nextThreadId; i++) {
                                addDdlCommand(i, order, defer);
                            }
                            order++;

                        } else if (REPEAT.equals(command)) {
                            String arg = line.substring(REPEAT_LEN).trim();
                            repeatCount = Integer.parseInt(vars.expand(arg));
                            trace("start @repeat block", repeatCount);
                            assert (repeatCount > 0) : "Repeat count must be > 0";
                            in.mark(REPEAT_READ_AHEAD_LIMIT);

                        } else if (END.equals(command)) {
                            if (SETUP_STATE.equals(state)) {
                                trace("end @setup");
                            } else if (CLEANUP_STATE.equals(state)) {
                                trace("end @cleanup");
                            } else if (THREAD_STATE.equals(state)) {
                                threadId = nextThreadId;
                            } else if (REPEAT_STATE.equals(state)) {
                                trace("repeating");
                                repeatCount--;
                                if (repeatCount > 0) {
                                    try {
                                        in.reset();
                                    } catch (IOException e) {
                                        throw new IllegalStateException(
                                            "Unable to reset reader -- repeat "
                                            + "contents must be less than "
                                            + REPEAT_READ_AHEAD_LIMIT + " bytes");
                                    }

                                    trace("end @repeat block");
                                    // don't let the state change
                                    continue;
                                }
                            } else {
                                assert (false);
                            }

                        } else if (SYNC.equals(command)) {
                            trace("@sync");
                            for (int i = threadId; i < nextThreadId; i++) {
                                addSynchronizationCommand(i, order);
                            }
                            order++;

                        } else if (TIMEOUT.equals(command)) {
                            String args = line.substring(TIMEOUT_LEN).trim();
                            String millisStr = vars.expand(firstWord(args));
                            long millis = Long.parseLong(millisStr);
                            assert (millis >= 0L) : "Timeout must be >= 0";

                            String sql =
                                readSql(skipFirstWord(args).trim(), in);
                            trace("@timeout", sql);
                            boolean isSelect = isSelect(sql);
                            for (int i = threadId; i < nextThreadId; i++) {
                                CommandWithTimeout cmd =
                                    isSelect
                                    ? new SelectCommand(sql, millis)
                                    : new SqlCommand(sql, millis);
                                addCommand(i, order, cmd);
                            }
                            order++;

                        } else if (ROWLIMIT.equals(command)) {
                            String args = line.substring(ROWLIMIT_LEN).trim();
                            String limitStr = vars.expand(firstWord(args));
                            int limit = Integer.parseInt(limitStr);
                            assert (limit >= 0) : "Rowlimit must be >= 0";

                            String sql =
                                readSql(skipFirstWord(args).trim(), in);
                            trace("@rowlimit ", sql);
                            boolean isSelect = isSelect(sql);
                            if (!isSelect) {
                                throw new IllegalStateException("Only select can be used with rowlimit");
                            }
                            for (int i = threadId; i < nextThreadId; i++) {
                                addCommand(
                                    i,
                                    order,
                                    new SelectCommand(sql, 0, limit));
                            }
                            order++;

                        } else if (PRINT.equals(command)) {
                            String spec =
                                vars.expand(line.substring(PRINT_LEN).trim());
                            trace("@print", spec);
                            for (int i = threadId; i < nextThreadId; i++) {
                                addCommand(i, order, new PrintCommand(spec));
                            }
                            order++;

                        } else if (PREPARE.equals(command)) {
                            String startOfSql =
                                line.substring(PREPARE_LEN).trim();
                            String sql = readSql(startOfSql, in);
                            trace("@prepare", sql);
                            for (int i = threadId; i < nextThreadId; i++) {
                                addCommand(i, order, new PrepareCommand(sql));
                            }
                            order++;

                        } else if (SHELL.equals(command)) {
                            String cmd = line.substring(SHELL_LEN).trim();
                            cmd = readLine(cmd, in);
                            trace("@shell", cmd);
                            for (int i = threadId; i < nextThreadId; i++) {
                                addCommand(i, order, new ShellCommand(cmd));
                            }
                            order++;

                        } else if (ECHO.equals(command)) {
                            String msg = line.substring(ECHO_LEN).trim();
                            msg = readLine(msg, in);
                            trace("@echo", msg);
                            for (int i = threadId; i < nextThreadId; i++) {
                                addCommand(i, order, new EchoCommand(msg));
                            }
                            order++;

                        } else if (ERR.equals(command)) {
                            String startOfSql =
                                line.substring(ERR_LEN).trim();
                            String sql = readSql(startOfSql, in);
                            trace("@err ", sql);
                            boolean isSelect = isSelect(sql);
                            for (int i = threadId; i < nextThreadId; i++) {
                                CommandWithTimeout cmd =
                                    isSelect
                                    ? new SelectCommand(sql, true)
                                    : new SqlCommand(sql, true);
                                addCommand(i, order, cmd);
                            }
                            order++;

                        } else if (FETCH.equals(command)) {
                            trace("@fetch");
                            String millisStr =
                                vars.expand(line.substring(FETCH_LEN).trim());
                            long millis = 0L;
                            if (millisStr.length() > 0) {
                                millis = Long.parseLong(millisStr);
                                assert (millis >= 0L) : "Fetch timeout must be >= 0";
                            }

                            for (int i = threadId; i < nextThreadId; i++) {
                                addCommand(
                                    i,
                                    order,
                                    new FetchAndPrintCommand(millis));
                            }
                            order++;

                        } else if (CLOSE.equals(command)) {
                            trace("@close");
                            for (int i = threadId; i < nextThreadId; i++) {
                                addCloseCommand(i, order);
                            }
                            order++;

                        } else if (SLEEP.equals(command)) {
                            String arg =
                                vars.expand(line.substring(SLEEP_LEN).trim());
                            trace("@sleep", arg);
                            long millis = Long.parseLong(arg);
                            assert (millis >= 0L) : "Sleep timeout must be >= 0";

                            for (int i = threadId; i < nextThreadId; i++) {
                                addSleepCommand(i, order, millis);
                            }
                            order++;

                        } else {
                            assert false : "Unknown command " + command;
                        }
                    }

                    String nextState = commandStateMap.get(command);
                    assert (nextState != null);
                    if (! nextState.equals(state)) {
                        doEndOfState(state);
                    }
                    state = nextState;
                }

                // at EOF
                currentDirectory.pop();
                if (currentDirectory.size() == 1) {
                    // at top EOF
                    if (!lookupState(state).containsKey(EOF)) {
                        throw new IllegalStateException(
                            "Premature end of file in '" + state + "' state");
                    }
                }
            } finally {
                in.close();
            }
        }

        private void doEndOfState(String state)
        {
            if (state.equals(PRE_SETUP_STATE)) {
                applyVariableRebindings();
            }
        }

        private void defineVariables(String line)
        {
            // two forms: "VAR VAR*" and "VAR=VAL$"
            Matcher varDefn = matchesVarDefn.matcher(line);
            if (varDefn.lookingAt()) {
                String var = varDefn.group(1);
                String val = varDefn.group(2);
                vars.define(var, val);
            } else {
                String[] words = splitWords.split(line);
                for (String var : words) {
                    String value = System.getenv(var);
                    vars.define(var, value);
                }
            }
        }


        /**
         * Manages state transitions.
         * Converts a state name into a map. Map keys are the names of available
         * commands (e.g. @sync), and map values are the state to switch to open
         * seeing the command.
         */
        private Map<String, String> lookupState(String state)
        {
            assert (state != null);

            for (int i = 0, n = STATE_TABLE.length; i < n; i++) {
                if (state.equals(STATE_TABLE[i].state)) {
                    StateDatum [] stateData = STATE_TABLE[i].stateData;

                    Map<String, String> result = new HashMap<String, String>();
                    for (int j = 0, m = stateData.length; j < m; j++) {
                        result.put(stateData[j].x, stateData[j].y);
                    }
                    return result;
                }
            }

            throw new IllegalArgumentException();
        }

        /**
         * Returns the first word of the given line, assuming the line is
         * trimmed. Returns the characters up the first non-whitespace
         * character in the line.
         */
        private String firstWord(String trimmedLine)
        {
            return trimmedLine.replaceFirst("\\s.*", "");
        }

        /**
         * Returns everything but the first word of the given line, assuming the
         * line is trimmed. Returns the characters following the first series of
         * consecutive whitespace characters in the line.
         */
        private String skipFirstWord(String trimmedLine)
        {
            return trimmedLine.replaceFirst("^\\S+\\s+", "");
        }

        /**
         * Returns an input line, possible extended by the continuation
         * character (\).  Scans the script until it finds an un-escaped
         * newline.
         */
        private String readLine(String line, BufferedReader in)
            throws IOException
        {
            line = line.trim();
            boolean more = line.endsWith("\\");
            if (more) {
                line = line.substring(0, line.lastIndexOf('\\')); // snip
                StringBuffer buf = new StringBuffer(line);        // save
                while (more) {
                    line = in.readLine();
                    if (line == null) {
                        break;
                    }
                    line = line.trim();
                    more = line.endsWith("\\");
                    if (more) {
                        line = line.substring(0, line.lastIndexOf('\\'));
                    }
                    buf.append(' ').append(line);
                }
                line = buf.toString().trim();
            }

            if (scriptHasVars && line.contains("$")) {
                line = vars.expand(line);
            }

            return line;
        }

        /**
         * Returns a block of SQL, starting with the given String. Returns
         * <code> startOfSql</code> concatenated with each line from
         * <code>in</code> until a line ending with a semicolon is found.
         */
        private String readSql(String startOfSql, BufferedReader in)
            throws IOException
        {
            // REVIEW mb StringBuffer not always needed
            StringBuffer sql = new StringBuffer(startOfSql);
            sql.append('\n');

            String line;
            if (!startOfSql.trim().endsWith(";")) {
                while ((line = in.readLine()) != null) {
                    sql.append(line).append('\n');
                    if (line.trim().endsWith(";")) {
                        break;
                    }
                }
            }

            line = sql.toString().trim();
            if (scriptHasVars && line.contains("$")) {
                line = vars.expand(line);
            }
            return line;
        }
    }


    // Inner Classes: the Commands

    // When executed, a @print command defines how any following @fetch
    // or @select commands will handle their resuult rows. MTSQL can print all
    // rows, no rows, or every nth row. A printed row can be prefixed by a
    // sequence nuber and/or the time it was received (a different notion than
    // its rowtime, which often tells when it was inserted).
    private class PrintCommand extends AbstractCommand
    {
        // print every nth row: 1 means all rows, 0 means no rows.
        private final int nth;
        private final boolean count;    // print a sequence number
        private final boolean time;     // print the time row was fetched
        // TODO: more control of formats

        PrintCommand(String spec)
        {
            int nth = 1;
            boolean count = false;
            boolean time = false;
            StringTokenizer tokenizer = new StringTokenizer(spec);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                if (token.equalsIgnoreCase("none")) {
                    nth = 0;
                } else if (token.equalsIgnoreCase("all")) {
                    nth = 1;
                } else if (token.equalsIgnoreCase("count")) {
                    count = true;
                } else if (token.equalsIgnoreCase("time")) {
                    time = true;
                } else if (token.equalsIgnoreCase("every")) {
                    nth = 1;
                    if (tokenizer.hasMoreTokens()) {
                        token = tokenizer.nextToken();
                        nth = Integer.parseInt(token);
                    }
                }
            }
            this.nth = nth;
            this.count = count;
            this.time = time;
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            Integer threadId = executor.getThreadId();
            BufferedWriter out = threadBufferedWriters.get(threadId);
            threadResultsReaders.put(
                threadId,
                new ResultsReader(out, nth, count, time));
        }
    }

    private class EchoCommand extends AbstractCommand
    {
        private final String msg;
        private EchoCommand(String msg)
        {
            this.msg = msg;
        }
        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            storeMessage(executor.getThreadId(), msg);
        }
    }

    // Matches shell wilcards and other special characters: when a command
    // contains some of these, it needs a shell to run it.
    private final Pattern shellWildcardPattern = Pattern.compile("[*?$]");


    // REVIEW mb 2/24/09 (Mardi Gras) Should this have a timeout?
    private class ShellCommand extends AbstractCommand
    {
        private final String command;
        private List<String> argv;      // the command, parsed and massaged

        private ShellCommand(String command)
        {
            this.command = command;
            boolean needShell = hasWildcard(command);
            if (needShell) {
                argv = new ArrayList<String>();
                argv.add("/bin/sh");
                argv.add("-c");
                argv.add(command);
            } else {
                argv = tokenize(command);
            }
        }

        private boolean hasWildcard(String command)
        {
            return shellWildcardPattern.matcher(command).find();
        }

        private List<String> tokenize(String s)
        {
            List<String> result = new ArrayList<String>();
            StringTokenizer tokenizer = new StringTokenizer(s);
            while (tokenizer.hasMoreTokens()) {
                result.add(tokenizer.nextToken());
            }
            return result;
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
        {
            Integer threadId = executor.getThreadId();
            storeMessage(threadId, command);

            // argv[0] is found on $PATH. Working directory is the script's home
            // directory.
            ProcessBuilder pb = new ProcessBuilder(argv);
            pb.directory(scriptDirectory);
            try {
                // direct stdout & stderr to the the threadWriter
                int status =
                    Util.runAppProcess(
                        pb, null, null, getThreadWriter(threadId));
                if (status != 0) {
                    storeMessage(
                        threadId,
                        "command " + command + ": exited with status " + status);
                }
            } catch (Exception e) {
                storeMessage(
                    threadId,
                    "command " + command + ": failed with exception " + e.getMessage());
            }
        }
    }


    // TODO: replace by super.CommmandWithTimeout
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
                assert (timeout >= 1000) : "timeout too short";
                stmt.setQueryTimeout((int) (timeout / 1000));
                return true;
            }

            return false;
        }
    }

    private static abstract class CommandWithTimeoutAndRowLimit
        extends CommandWithTimeout
    {
        private int rowLimit;

        private CommandWithTimeoutAndRowLimit(long timeout)
        {
            this(timeout, 0);
        }

        private CommandWithTimeoutAndRowLimit(long timeout, int rowLimit)
        {
            super(timeout);
            this.rowLimit = rowLimit;
        }

        protected void setRowLimit(Statement stmt)
            throws SQLException
        {
            assert (rowLimit >= 0);
            if (rowLimit > 0) {
                stmt.setMaxRows(rowLimit);
            }
        }
    }

    /**
     * SelectCommand creates and executes a SQL select statement, with optional
     * timeout and row limit.
     */
    private class SelectCommand
        extends CommandWithTimeoutAndRowLimit
    {
        private String sql;

        private SelectCommand(String sql)
        {
            this(sql, 0, 0);
        }

        private SelectCommand(String sql, boolean errorExpected)
        {
            this(sql, 0, 0);
            if (errorExpected) {
                this.markToFail();
            }
        }

        private SelectCommand(String sql, long timeout)
        {
            this(sql, timeout, 0);
        }

        private SelectCommand(String sql, long timeout, int rowLimit)
        {
            super(timeout, rowLimit);
            this.sql = sql;
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            // TODO: trim and chop in constructor; stash sql in base class;
            // execute() calls storeSql.
            String properSql = sql.trim();

            storeSql(
                executor.getThreadId(),
                properSql);

            if (properSql.endsWith(";")) {
                properSql = properSql.substring(0, properSql.length() - 1);
            }

            PreparedStatement stmt =
                executor.getConnection().prepareStatement(properSql);

            boolean timeoutSet = setTimeout(stmt);
            setRowLimit(stmt);

            try {
                storeResults(
                    executor.getThreadId(),
                    stmt.executeQuery(),
                    timeoutSet);
            } finally {
                stmt.close();
            }
        }
    }

    /**
     * SelectCommand creates and executes a SQL select statement, with optional
     * timeout.
     */
    private class SqlCommand
        extends CommandWithTimeout
    {
        private String sql;

        private SqlCommand(String sql)
        {
            super(0);
            this.sql = sql;
        }

        private SqlCommand(String sql, boolean errorExpected)
        {
            super(0);
            this.sql = sql;
            if (errorExpected) {
                this.markToFail();
            }
        }

        private SqlCommand(String sql, long timeout)
        {
            super(timeout);
            this.sql = sql;
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            String properSql = sql.trim();

            storeSql(
                executor.getThreadId(),
                properSql);

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
                boolean haveResults = stmt.execute();
                if (haveResults) {
                    // Farrago rewrites "call" statements as selects.
                    storeMessage(
                        executor.getThreadId(),
                        "0 rows affected.");
                    // is there anything interesting in the ResultSet?
                } else {
                    int rows = stmt.getUpdateCount();
                    if (rows != 1) {
                        storeMessage(
                            executor.getThreadId(),
                            String.valueOf(rows) + " rows affected.");
                    } else {
                        storeMessage(
                            executor.getThreadId(),
                            "1 row affected.");
                    }
                }
            } catch (AbstractIterResultSet.SqlTimeoutException e) {
                if (!timeoutSet) {
                    throw e;
                }

                Util.swallow(e, null);
                storeMessage(
                    executor.getThreadId(),
                    "Timeout");
            } finally {
                stmt.close();
            }
        }
    }

    /**
     * PrepareCommand creates a {@link PreparedStatement}, which is saved as the
     * current statement of its test thread. For a preparted query (a SELECT or
     * a CALL with results), a subsequent FetchAndPrintCommand executes the
     * statement and fetches its reults, until end-of-data or a timeout. A
     * PrintCommand attaches a listener, called for each rows, that selects rows
     * to save and print, and sets the format. By default, if no PrintCommand
     * appears before a FetchAndPrintCommand, all rows are printed. A
     * CloseCommand closes and discards the prepared statement.
     */
    private class PrepareCommand
        extends AbstractCommand
    {
        private String sql;

        private PrepareCommand(String sql)
        {
            this.sql = sql;
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            String properSql = sql.trim();

            storeSql(
                executor.getThreadId(),
                properSql);

            if (properSql.endsWith(";")) {
                properSql = properSql.substring(0, properSql.length() - 1);
            }

            PreparedStatement stmt =
                executor.getConnection().prepareStatement(properSql);

            executor.setStatement(stmt);
        }
    }

    /**
     * FetchAndPrintCommand executes a previously prepared statement stored
     * inthe ConcurrentTestCommandExecutor and then outputs the returned
     * rows.
     */
    private class FetchAndPrintCommand
        extends CommandWithTimeout
    {
        private FetchAndPrintCommand(long timeout)
        {
            super(timeout);
        }

        protected void doExecute(ConcurrentTestCommandExecutor executor)
            throws SQLException
        {
            PreparedStatement stmt =
                (PreparedStatement) executor.getStatement();

            boolean timeoutSet = setTimeout(stmt);

            storeResults(
                executor.getThreadId(),
                stmt.executeQuery(),
                timeoutSet);
        }
    }

    private class ResultsReader
    {
        private final PrintWriter out;

        /** print every Nth row. 1 means all rows, 0 means none. */
        private final int nth;

        /** prefix printed row with its sequence number */
        private final boolean counted;

        /** prefix printed row with time it was fetched */
        private final boolean timestamped;

        private long baseTime = 0;
        private int ncols = 0;
        private int[] widths;
        private String[] labels;

        ResultsReader(BufferedWriter out)
        {
            this(out, 1, false, false);
        }

        ResultsReader(
            BufferedWriter out,
            int nth,
            boolean counted,
            boolean timestamped)
        {
            this.out = new PrintWriter(out);
            this.nth = nth;
            this.counted = counted;
            this.timestamped = timestamped;
            this.baseTime = executionStartTime;
        }

        void prepareFormat(ResultSet rset) throws SQLException
        {
            ResultSetMetaData meta = rset.getMetaData();
            ncols = meta.getColumnCount();
            widths = new int[ncols];
            labels = new String[ncols];
            for (int i = 0; i < ncols; i++) {
                labels[i] = meta.getColumnLabel(i + 1);
                int displaySize = meta.getColumnDisplaySize(i + 1);

                // NOTE jvs 13-June-2006: I put this in to cap EXPLAIN PLAN,
                // which now returns a very large worst-case display size.
                if (displaySize > 4096) {
                    displaySize = 0;
                }
                widths[i] = Math.max(labels[i].length(), displaySize);
            }
        }

        private void printHeaders()
        {
            printSeparator();
            indent(); printRow(labels);
            printSeparator();
        }

        void read(ResultSet rset, boolean withTimeout) throws SQLException
        {
            try {
                prepareFormat(rset);
                String [] values = new String[ncols];
                int printedRowCount = 0;
                if (nth > 0) {
                    printHeaders();
                }
                for (int rowCount = 0; rset.next(); rowCount++) {
                    if (nth == 0) {
                        continue;
                    }
                    if (nth == 1 || rowCount % nth == 0) {
                        long time = System.currentTimeMillis();
                        if (printedRowCount > 0
                            && (printedRowCount % 100 == 0))
                        {
                            printHeaders();
                        }
                        for (int i = 0; i < ncols; i++) {
                            values[i] = rset.getString(i + 1);
                        }
                        if (counted) {
                            printRowCount(rowCount);
                        }
                        if (timestamped) {
                            printTimestamp(time);
                        }
                        printRow(values);
                        printedRowCount++;
                    }
                }
            } catch (AbstractIterResultSet.SqlTimeoutException e) {
                if (!withTimeout) {
                    throw e;
                }
                Util.swallow(e, null);
            } catch (SQLException e) {
                // 2007-10-23 hersker: hack to ignore timeout exceptions
                // from other Farrago projects without being able to
                // import/reference the actual exceptions
                final String eClassName = e.getClass().getName();
                if (eClassName.endsWith("TimeoutException")) {
                    if (!withTimeout) {
                        throw e;
                    }
                    Util.swallow(e, null);
                } else {
                    Util.swallow(e, null);
                    out.println(e.getMessage());
                }
            } catch (RuntimeException e) {
                e.printStackTrace();
                throw e;
            } finally {
                rset.close();
                if (nth > 0) {
                    printSeparator();
                    out.println();
                }
            }
        }

        private void printRowCount(int count)
        {
            out.printf("(%06d) ", count);
        }

        private void printTimestamp(long time)
        {
            time -= baseTime;
            out.printf("(% 4d.%03d) ", time / 1000, time % 1000);
        }

        // indent a heading or separator line to match a row-values line
        private void indent()
        {
            if (counted) {
                out.print("         ");
            }
            if (timestamped) {
                out.print("           ");
            }
        }

        /**
         * Prints an output table separator. Something like <code>
         * "+----+--------+"</code>.
         */
        private void printSeparator()
        {
            indent();
            for (int i = 0; i < widths.length; i++) {
                if (i > 0) {
                    out.write("-+-");
                } else {
                    out.write("+-");
                }

                int numDashes = widths[i];
                while (numDashes > 0) {
                    out.write(
                        dashes,
                        0,
                        Math.min(numDashes, BUF_SIZE));
                    numDashes -= Math.min(numDashes, BUF_SIZE);
                }
            }
            out.println("-+");
        }

        /**
         * Prints an output table row. Something like <code>"| COL1 | COL2
         * |"</code>.
         */
        private void printRow(String [] values)
        {
            for (int i = 0; i < values.length; i++) {
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
                while (excess > 0) {
                    out.write(
                        spaces,
                        0,
                        Math.min(excess, BUF_SIZE));
                    excess -= Math.min(excess, BUF_SIZE);
                }
            }
            out.println(" |");
        }
    }


    // Inner class: stand-alone client test tool
    static private class Tool
    {
        boolean quiet = false;          // -q
        boolean verbose = false;        // -v
        boolean debug = false;          // -g
        String server;                  // -u
        String driver;                  // -d
        String user;                    // -n
        String password;                // -p
        List<String> bindings;          // VAR=VAL
        List<String> files;             // FILE

        public Tool()
        {
            bindings = new ArrayList<String>();
            files = new ArrayList<String>();
        }

        // returns 0 on success, 1 on error, 2 on bad invocation.
        public int run(String[] args)
        {
            try {
                if (!parseCommand(args)) {
                    usage();
                    return 2;
                }

                Class z = Class.forName(driver); // load driver
                Properties jdbcProps = new Properties();
                if (user != null) {
                    jdbcProps.setProperty("user", user);
                }
                if (password != null) {
                    jdbcProps.setProperty("password", password);
                }

                BufferedWriter cout =
                    new BufferedWriter(new OutputStreamWriter(System.out));
                for (String file : files) {
                    ConcurrentTestCommandScript script =
                        new ConcurrentTestCommandScript();
                    script.setQuiet(quiet);
                    script.setVerbose(verbose);
                    script.setDebug(debug);
                    script.prepare(file, bindings);
                    script.setDataSource(server, jdbcProps);
                    script.execute();
                    if (!quiet) {
                        script.printResults(cout);
                    }
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
                return 1;
            }
            return 0;
        }

        static void usage()
        {
            System.err.println("Usage: mtsql [-vg] -u SERVER -d DRIVER [-n USER][-p PASSWORD] SCRIPT [SCRIPT]...");
        }

        boolean parseCommand(String[] args)
        {
            try {
                // very permissive as to order
                for (int i = 0; i < args.length;) {
                    String arg = args[i++];
                    if (arg.charAt(0) == '-') {
                        switch (arg.charAt(1)) {
                        case 'v':
                            verbose = true;
                            break;
                        case 'q':
                            quiet = true;
                            break;
                        case 'g':
                            debug = true;
                            break;
                        case 'u':
                            this.server = args[i++];
                            break;
                        case 'd':
                            this.driver = args[i++];
                            break;
                        case 'n':
                            this.user = args[i++];
                            break;
                        case 'p':
                            this.password = args[i++];
                            break;
                        default:
                            return false;
                        }
                    } else if (arg.contains("=")) {
                        if (Character.isJavaIdentifierStart(arg.charAt(0))) {
                            bindings.add(arg);
                        } else {
                            return false;
                        }
                    } else {
                        files.add(arg);
                    }
                }
                if (server == null || driver == null) {
                    return false;
                }
            } catch (Throwable th) {
                return false;
            }
            return true;
        }
    }

    /**
     * Client tool that connects via jdbc and runs one or more mtsql on that
     * connection.
     *
     * <p>Usage: mtsql [-vgq] -u SERVER -d DRIVER [-n USER][-p PASSWORD]
     * [VAR=VAL]...  SCRIPT [SCRIPT]...
     */
    static public void main(String[] args)
    {
        int status = new Tool().run(args);
        System.exit(status);
    }
}

// End ConcurrentTestCommandScript.java
