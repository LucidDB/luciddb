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

package org.luciddb.pg2luciddb;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.sql.Driver;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.Vector;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;

import org.h2.message.Message;
import org.h2.util.ByteUtils;
import org.h2.util.IOUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.New;
import org.h2.util.ScriptReader;
import org.h2.tools.SimpleResultSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import java.security.MessageDigest;


/**
 * One server thread is opened for each client.
 */
public class ServerThread implements Runnable 
{
    private static final int TYPE_STRING = Types.VARCHAR;
    private Server server;
    private Socket socket;
    private Connection conn;
    private boolean stop;
    private DataInputStream dataInRaw;
    private DataInputStream dataIn;
    private OutputStream out;
    private int messageType;
    private ByteArrayOutputStream outBuffer;
    private DataOutputStream dataOut;
    private Thread thread;
    private boolean initDone;
    private String userName;
    private String databaseName;
    private int processId;
    private String clientEncoding = "UTF-8";
    private String dateStyle = "ISO";
    private HashMap<String, Prepared> prepared = New.hashMap();
    private HashMap<String, Portal> portals = New.hashMap();
    private HashSet<Integer> types = New.hashSet();

    // get logger class:
    private static final Logger logger = Logger.getLogger(ServerThread.class);

    // protocol V3 (for secure auth purposes):
    private static final int PROTOCOL_V3 = 196608;

    // salt & secret key:
    private String salt;
    private int secretKey;

    // JDBC date constants:
    private static final long JDBC_DATE_INFINITY = 9223372036825200000l;
    private static final long JDBC_DATE_MINUS_INFINITY = -9223372036832400000l;
    private static final String POSTGRES_DATE_INFINITY = "infinity";
    private static final String POSTGRES_DATE_MINUS_INFINITY = "-infinity";

    // regexp:
    private static final String SELECT_PGTYPE_PATTERN_STRING = "^(select.*\\s*from)\\s*(pg_type)(.*?)$";
    private static final String COLUMNS_Q_SEARCH_BY_IDS_1_STRING = "and\\s*c[.]oid\\s*=(\\d+)\\s*"; 
    private static final String COLUMNS_Q_SEARCH_BY_IDS_2_STRING = "and\\s*a[.]attnum\\s*=\\s*(\\d+)\\s*";
    private static final String COLUMNS_Q_TABLE_STRING = "c[.]relname\\s*(like|=)\\s*[EN][']([^']*)[']";
    private static final String COLUMNS_Q_SCHEMA_STRING = "n[.]nspname\\s*(like|=)\\s*[EN][']([^']*)[']";

    private static final String TABLES_Q1_STRING = "relname\\s*(like|=)\\s*[EN][']([^']*)[']";
    private static final String TABLES_Q2_STRING = "nspname\\s*(like|=)\\s*[EN][']([^']*)[']";

    // pattern to escape E' quoted values:
    private static final String E_ESCAPING_STRING = "[E]'(([^']|''.)*)'";
    // pattern to find / remove PG-style type casting (::<type>), only basic types are supported for the moment:
    private static final String TYPE_CASTING_STRING = "'(([^']|''.)*)'::(int2|int4|int8|float4|float8|bool|boolean|numeric)";
    // for Npgsql provider:
    private static final String TYPE_CASTING_STRING2 = "[(]'(([^']|''.)*)'[)]::(int2|int4|int8|float4|float8|bool|boolean|numeric)";
    // typecasting for time, timestamp & date formats (using cast):
    private static final String TYPE_CASTING_STRING_DT_2 = "[(]'(([^']|''.)*)'[)]::(timestamp|time|date)";
    private static final String TYPE_CASTING_STRING_DT_1 = "'(([^']|''.)*)'::(timestamp|time|date)";
       
    // compiled patterns:
    private static final Pattern SELECT_PGTYPE_PATTERN = Pattern.compile(SELECT_PGTYPE_PATTERN_STRING, Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
    private static final Pattern COLUMNS_Q_SEARCH_BY_IDS_1 = Pattern.compile(COLUMNS_Q_SEARCH_BY_IDS_1_STRING, Pattern.DOTALL | Pattern.CASE_INSENSITIVE); 
    private static final Pattern COLUMNS_Q_SEARCH_BY_IDS_2 = Pattern.compile(COLUMNS_Q_SEARCH_BY_IDS_2_STRING, Pattern.DOTALL | Pattern.CASE_INSENSITIVE); 
    private static final Pattern COLUMNS_Q_TABLE = Pattern.compile(COLUMNS_Q_TABLE_STRING, Pattern.DOTALL | Pattern.CASE_INSENSITIVE); 
    private static final Pattern COLUMNS_Q_SCHEMA = Pattern.compile(COLUMNS_Q_SCHEMA_STRING, Pattern.DOTALL | Pattern.CASE_INSENSITIVE); 

    private static final Pattern TABLES_Q1 = Pattern.compile(TABLES_Q1_STRING, Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
    private static final Pattern TABLES_Q2 = Pattern.compile(TABLES_Q2_STRING, Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
    private static final Pattern E_ESCAPING = Pattern.compile(E_ESCAPING_STRING, Pattern.DOTALL);
    private static final Pattern TYPE_CASTING = Pattern.compile(TYPE_CASTING_STRING, Pattern.DOTALL);
    private static final Pattern TYPE_CASTING2 = Pattern.compile(TYPE_CASTING_STRING2, Pattern.DOTALL);

    private static final Pattern TYPE_CASTING_DT_1 = Pattern.compile(TYPE_CASTING_STRING_DT_1, Pattern.DOTALL);
    private static final Pattern TYPE_CASTING_DT_2 = Pattern.compile(TYPE_CASTING_STRING_DT_2, Pattern.DOTALL);
    
    // constructor:
    ServerThread(Socket socket, Server server) 
    {
        this.server = server;
        this.socket = socket;
    }

    // run thread:
    public void run() 
    {
        try 
        {
            server.trace("Connect");

            // generate salt & secret key:
            salt = generateRandomString(4);      
            Random random = new Random();
            secretKey = random.nextInt();

            logger.debug("Start serving client: " + socket.getRemoteSocketAddress());

            // set tcp parameters:
            socket.setSoTimeout(server.configuration.getConnectionCloseIdleTimeout());

            InputStream ins = socket.getInputStream();
            out = socket.getOutputStream();
            dataInRaw = new DataInputStream(ins);
            while (!stop) 
            {
                process();
                out.flush();
            }
        } 
        catch (EOFException e) 
        {
            // more or less normal disconnect
        } 
        catch (Exception e) 
        {
            logger.trace("Exception in [run] method: " + e.toString());
        } 
        finally 
        {
            server.trace("Disconnect");
            close();
        }
    }

    // read string from input buffer:
    private String readString() throws IOException 
    {
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        while (true) {
            int x = dataIn.read();
            if (x <= 0) {
                break;
            }
            buff.write(x);
        }
        return new String(buff.toByteArray(), getEncoding());
    }

    // read integer:
    private int readInt() throws IOException 
    {
        return dataIn.readInt();
    }

    // read short:
    private int readShort() throws IOException 
    {
        return dataIn.readShort();
    }

    // read byte:
    private byte readByte() throws IOException 
    {
        return dataIn.readByte();
    }

    // read array of bytes:
    private void readFully(byte[] buff) throws IOException 
    {
        dataIn.readFully(buff);
    }

    // generate a random string. This is used for password encryption.
    public static String generateRandomString(int count) 
    {
        Random random = new Random();
        StringBuffer buffer = new StringBuffer();

        while (count-- != 0) 
        {
            // a random number in the 32...127 range
            char ch = (char) (random.nextInt(96) + 32);
            buffer.append(ch);
        }

        return buffer.toString();
    }

    // process:
    private void process() throws IOException 
    {
        int x;
        // if init done:
        if (initDone) 
        {
            x = dataInRaw.read();
            if (x < 0) {
                stop = true;
                return;
            }
        } else {
            x = 0;
        }
        int len = dataInRaw.readInt();
        len -= 4;
        byte[] data = ByteUtils.newBytes(len);
        dataInRaw.readFully(data, 0, len);
        dataIn = new DataInputStream(new ByteArrayInputStream(data, 0, len));
        switch (x) {
        // init state:
        case 0:
            logger.trace("Init");
            int version = readInt();
            // cancel request:
            if (version == 80877102) 
            {
                logger.trace("CancelRequest (not supported)");
                logger.trace(" pid: " + readInt());
                logger.trace(" key: " + readInt());
            } 
            // SSL request:
            else if (version == 80877103) 
            {
                logger.trace("SSLRequest");
                out.write('N');
            } 
            // startup message:
            else 
            {
                logger.trace("StartupMessage");
                logger.trace(" version " + version + " (" + (version >> 16) + "." + (version & 0xff) + ")");
                while (true) 
                {
                    String param = readString();
                    if (param.length() == 0) {
                        break;
                    }
                    String value = readString();
                    if ("user".equals(param)) 
                    {
                        this.userName = value;
                    } 
                    else if ("database".equals(param)) 
                    {
                        this.databaseName = value;
                    } 
                    else if ("client_encoding".equals(param)) 
                    {
                        clientEncoding = value;
                    } 
                    else if ("DateStyle".equals(param)) 
                    {
                        dateStyle = value;
                    }
                    // server.log(" param " + param + "=" + value);
                }

                // request clear text password:
                //sendAuthenticationCleartextPassword();

                // if user doesn't have password:
                if (server.configuration.getAllowedUsers().get(this.userName) != null && server.configuration.getAllowedUsers().get(this.userName).toString().length() == 0)
                {
                    try
                    {
                       Properties credentials = new Properties();
                       credentials.put("user", userName);
                       credentials.put("password", "");
                       conn = server.configuration.getDriver().connect(server.configuration.getJdbcDriverBaseURL() + server.configuration.getJdbcDriverOptionsForUser(this.userName), credentials);

                       // set search path:
                       Statement _stat = null;
                       try
                       {
                           _stat = conn.createStatement();
                           _stat.execute("set path 'pg_catalog'");

                       }
                       catch(Exception ex)
                       {
                           JdbcUtils.closeSilently(_stat); 
                       }

                       // send auth ok:
                       sendAuthenticationOk();
                    }
                    catch (SQLException e) 
                    {
                       logger.error(e.toString());
                       sendErrorResponse(e);
                       stop = true;
                    }
                    catch(Exception e)
                    {
                       logger.error(e.toString());
                       stop = true;
                    }
                }
                else
                {
                    // request secure auth session:
                    sendAuthenticationMD5Password();
                }

                initDone = true;
            }
            break;
        // password:
        case 'p': 
        {
            logger.trace("PasswordMessage");

            // read cleartext password:                         
            String password = readString();

            String tmp = readString();

            logger.trace(tmp);

            try 
            {                
                // check if user is able to authenticate:
                if (server.configuration.getAllowedUsers().get(userName) != null && ((passwordsAreEqual(userName, server.configuration.getAllowedUsers().get(userName).toString(), password) && server.configuration.getAllowedUsers().get(userName).toString().length() > 0) ))
                {  
                    Properties credentials = new Properties();
                    credentials.put("user", userName);
                    credentials.put("password", server.configuration.getAllowedUsers().get(userName).toString());
                    conn = server.configuration.getDriver().connect(server.configuration.getJdbcDriverBaseURL() + server.configuration.getJdbcDriverOptionsForUser(userName), credentials);

                    // set search path:
                    Statement _stat = null;
                    try
                    {
                        _stat = conn.createStatement();
                        _stat.execute("set path 'pg_catalog'");

                    }
                    catch(Exception ex)
                    {
                        JdbcUtils.closeSilently(_stat); 
                    }

                    // send auth ok:
                    sendAuthenticationOk();
                }
                else
                {
                    logger.trace("Password does NOT match!");
                    sendErrorResponse("08004", "Password does not match!");
                    stop = true;
                }                  
            } 
            catch (SQLException e) 
            {
                logger.error(e.toString());
                sendErrorResponse(e);
                stop = true;
            }
            catch(Exception e)
            {
                logger.error(e.toString());
                stop = true;
            }
            break;
        }
        // parse query:
        case 'P': 
        {
            server.trace("Parse");
            Prepared p = new Prepared();
            p.name = readString();
            // TODO: do we need to execute seperated method here ???
            p.sql = getSQL(readString(), true);

            // get number of parameters:
            int count = readShort();
            p.paramType = new int[count];
            p.parameters = new String[count];

            // set parameters:
            for (int i = 0; i < count; i++) 
            {
                int type = readInt();
                checkType(type);
                p.paramType[i] = type;
            }
            // try to prepare statement:
            try 
            {
                // right now - execute ALL statements:
                PreparedStatement pst = conn.prepareStatement(p.sql);
                pst.close();
                prepared.put(p.name, p);
                sendParseComplete();
            } 
            catch (SQLException e) 
            {
                sendErrorResponse(e);
            }

            break;
        }
        // bind:
        case 'B': 
        {
            server.trace("Bind");
            Portal portal = new Portal();
            portal.name = readString();
            String prepName = readString();
            Prepared prep = prepared.get(prepName);
            if (prep == null) 
            {
                sendErrorResponse("Portal not found");
                break;
            }

            portal.sql = prep.sql;
            portal.prepared = prep;
            portals.put(portal.name, portal);

            int formatCodeCount = readShort();
            int[] formatCodes = new int[formatCodeCount];

            // get format codes:
            for (int i = 0; i < formatCodeCount; i++) 
            {
                formatCodes[i] = readShort();
            }

            int paramCount = readShort();
            // if parameters not set earlier:
            if (portal.prepared.parameters == null || portal.prepared.parameters.length == 0)
            {
                portal.prepared.parameters = new String[paramCount];
            }

            // set parameters:
            for (int i = 0; i < paramCount; i++) 
            {
                int paramLen = readInt();
                byte[] d2 = ByteUtils.newBytes(paramLen);
                readFully(d2);
                try 
                {
                    setParameter(portal.prepared, i, d2, formatCodes);
                } 
                catch (SQLException e) 
                {
                    sendErrorResponse(e);
                }
            }

            int resultCodeCount = readShort();
            portal.resultColumnFormat = new int[resultCodeCount];
            for (int i = 0; i < resultCodeCount; i++) 
            {
                portal.resultColumnFormat[i] = readShort();
            }
            sendBindComplete();
            break;
        }
        // describe:
        case 'D':                          
        {
            char type = (char) readByte();
            String name = readString();
            logger.trace("Describe");
            if (type == 'S') 
            {
                Prepared p = prepared.get(name);
                if (p == null) 
                {
                    sendErrorResponse("Prepared not found: " + name);
                } 
                else 
                {
                    sendParameterDescription(p);
                }
            } 
            else if (type == 'P') 
            {
                Portal p = portals.get(name);
                if (p == null) 
                {
                    sendErrorResponse("Portal not found: " + name);
                } 
                else 
                {                    
                    try 
                    {
                        // if dummy function call - return no data:
                        if (p.sql.startsWith("CALL PG_CATALOG.DUMMY_PROCEDURE"))
                        {
                            sendRowDescription(null);
                        }
                        // else:
                        else     
                        {
                            PreparedStatement prep = conn.prepareStatement(p.sql);
                            ResultSetMetaData meta = prep.getMetaData();
                            sendRowDescription(meta);
                            prep.close();
                        }
                    } 
                    catch (SQLException e) 
                    {
                        sendErrorResponseNoLog(e);
                    }
                }           
            } 
            else 
            {
                logger.trace("expected S or P, got " + type);
                sendErrorResponse("expected S or P");
            }
            break;
        }
        // execute prepare query:
        case 'E': 
        {
            String name = readString();
            server.trace("Execute");
            Portal p = portals.get(name);
            if (p == null) 
            {
                sendErrorResponse("Portal not found: " + name);
                break;
            }
            int maxRows = readShort();

            try 
            {

                PreparedStatement prep = conn.prepareStatement(p.sql);
                // set parameters:
                for (int i = 0; i < p.prepared.parameters.length; i++)
                {
                     prep.setString(i + 1, p.prepared.parameters[i]);
                }                                
          
                logger.trace(p.sql);
                prep.setMaxRows(maxRows);                
                boolean result = prep.execute();
                if (result) 
                {
                    try 
                    {
                        ResultSet rs = prep.getResultSet();
                        ResultSetMetaData meta = rs.getMetaData();
                        sendRowDescription(meta);
                        while (rs.next()) 
                        {
                            sendDataRow(rs);
                        }
                        rs.close();
                        sendCommandComplete(p.sql, 0);
                    } 
                    catch (SQLException e) 
                    {
                        sendErrorResponse(e);
                    }
                } 
                else 
                {
                    sendCommandComplete(p.sql, prep.getUpdateCount());
                }

                prep.close();
            } 
            catch (SQLException e) 
            {
                sendErrorResponse(e);
            }
            break;
        }
        // sync query:
        case 'S': 
        {
            logger.trace("Sync");
            sendReadyForQuery();
            break;
        }
        // query:
        case 'Q': 
        {
            logger.trace("Query");
            String query = readString();

            ScriptReader reader = new ScriptReader(new StringReader(query));
            while (true) 
            {
                Statement stat = null;
                try 
                {
                    String s = reader.readStatement();                    
                    if (s == null) 
                    {
                        break;
                    }
                    String _s = new String(s);

                    // get statement (execute, not parse):
                    s = getSQL(s, false);

                    // FIX: do not process empty query:
                    if (s == null || s == "")
                    {
                        // do not break if BEGIN or START TRANSACTION block
                        if (!_s.startsWith("BEGIN") || !_s.startsWith("START TRANSACTION"))
                            break;
                        else
                            continue;
                    }

                    // FIX: here we can jump into pipeline
                    //if (s.startsWith("say hello"))
                    //{
                    //    // generate simple resultset on the fly:
              	    //    SimpleResultSet rs = new SimpleResultSet();
         	    //    rs.addColumn("Response", Types.VARCHAR, 255, 0);
        	    //    rs.addRow(new Object[] { "Hello, world!" });
        	    //
                    //    ResultSetMetaData meta = rs.getMetaData();
                    //    sendRowDescription(meta);
                    //    while (rs.next()) 
                    //    {
                    //        sendDataRow(rs);
                    //    }
                    //    sendCommandComplete(s, 0);
                    //}

                    stat = conn.createStatement();
                    boolean result = stat.execute(s);
                    if (result) 
                    {
                        ResultSet rs = stat.getResultSet();
                        ResultSetMetaData meta = rs.getMetaData();
                        sendRowDescription(meta);
                        while (rs.next()) 
                        {
                            sendDataRow(rs);
                        }
                        sendCommandComplete(s, 0);
                    } 
                    else 
                    {
                        sendCommandComplete(s, stat.getUpdateCount());
                    }
                } 
                catch (SQLException e) 
                {
                    sendErrorResponse(e);
                } 
                finally 
                {
                    JdbcUtils.closeSilently(stat);
                }
            }
            sendReadyForQuery();
            break;
        }
        // terminate connection:
        case 'X': 
        {
            logger.trace("Terminate");
            close();
            break;
        }
        // unsupported query:
        default:
            logger.trace("Unsupported: " + x + " (" + (char) x + ")");
            break;
        }
    }

    // replace parameters with question marks:
    public static String replaceParametersWithQuestionMarks(String statement) 
    {
        /* TODO handle parameters surrounded with quote */

        if (statement == null) {
            return null;
        }

        String result = "";
        int i = 0;
        int len = statement.length();
        char last = '\0';

        while (i < len) {
            char c = statement.charAt(i);
            if (c == '$') {
                last = c;
                i++;
                continue;
            }
            if ((last == '$') && (c >= '0') && (c <= '9')) {
                i++;
                while ((i < len) && (Character.isDigit(statement.charAt(i)))) {
                    i++;
                }
                last = '\0';
                c = '?';
                i--;
            }
            result += c;
            i++;
        }
        return result;
    }

    // check type:
    private void checkType(int type) 
    {
        if (types.contains(type)) 
        {
            logger.trace("Unsupported type: " + type);
        }
    }

    // replace all:
    public static String replaceAll(String s, String before, String after) 
    {
        StringBuilder buff = new StringBuilder(s.length());
        int index = 0;
        while (true) 
        {
            int next = s.indexOf(before, index);
            if (next < 0) {
                buff.append(s.substring(index));
                break;
            }
            buff.append(s.substring(index, next)).append(after);
            index = next + before.length();
        }
        return buff.toString();
    }
   
    // get sql (preprocess query before execution):
    private String getSQL(String s, Boolean parseStatement) throws IOException
    {
        // trace:
        logger.trace("Original query: " + s);

        // if parse statement - replace params:
        if (parseStatement)
        {
            s = replaceParametersWithQuestionMarks(s);            
        }
        else
        {
            // deallocate statement:
            if (s.startsWith("DEALLOCATE ")) 
            {
                String name = s.substring(11).trim(); // remove "DEALLOCATE"
                if (name.startsWith("PREPARE"))
                    name = name.substring(8).trim(); // remove "PREPARE" if any
                
                if (prepared.get(name) != null)
                {
                    prepared.put(name, null);
                }

                sendCommandComplete(s, 0);
                s = "";
                return s;
            }
        }

        // convert statement to lower case:
        String lower = s.toLowerCase();    
  
        // ignore max_identifier_length:  
        if (lower.startsWith("show max_identifier_length")) 
        {
            if (!parseStatement)
            {
               try
               {
                   // send dataset:
                   SimpleResultSet rs = new SimpleResultSet();
                   rs.addColumn("max_identifier_length", Types.VARCHAR, 255, 0);
     	           rs.addRow(new Object[] { "63" });

                   ResultSetMetaData meta = rs.getMetaData();
                   sendRowDescription(meta);
                   while (rs.next()) 
                   {
                      sendDataRow(rs);
                   }
                   sendCommandComplete(s, 0);
                   s = "";
               }
               catch(Exception e) {} 
            }
        } 
        // ignore show escape_string_warning:  
        else if (lower.startsWith("show escape_string_warning")) 
        {
            if (!parseStatement)
            {
               try
               {
                   // send dataset:
                   SimpleResultSet rs = new SimpleResultSet();
                   rs.addColumn("escape_string_warning", Types.VARCHAR, 255, 0);
     	           rs.addRow(new Object[] { "on" });

                   ResultSetMetaData meta = rs.getMetaData();
                   sendRowDescription(meta);
                   while (rs.next()) 
                   {
                      sendDataRow(rs);
                   }
                   sendCommandComplete(s, 0);
                   s = "";
               }
               catch(Exception e) {} 
            }
        } 

        // ignore setting encoding:
        else if (lower.startsWith("set client_encoding to")) 
        {
            if (!parseStatement)
            {
                sendCommandComplete(s, 0);
                s = "";
            }
            else
            {
                s = "CALL PG_CATALOG.DUMMY_PROCEDURE('')";
            }
        }
        // ignore all LISTEN, NOTIFY & transaction-related commands:
        else if (lower.startsWith("begin") || lower.startsWith("end") || lower.startsWith("commit") || lower.startsWith("start transaction") || lower.startsWith("rollback") || lower.startsWith("notify") || lower.startsWith("listen") || lower.startsWith("unlisten"))        
        {
            if (!parseStatement)
            {
                sendCommandComplete(s, 0);
                s = "";
            }
            // else - replace with relly callable statement:
            else
            {
                // start transaction:
                if (lower.startsWith("begin") || lower.startsWith("start transaction"))
                {                
                    s = "CALL PG_CATALOG.DUMMY_PROCEDURE('START TRANSACTION')";
                }
                // commit:
                else if (lower.startsWith("commit") || lower.startsWith("end"))
                {
                    s = "CALL PG_CATALOG.DUMMY_PROCEDURE('COMMIT')";
                }
                // rollback:
                else if (lower.startsWith("rollback"))
                {
                    s = "CALL PG_CATALOG.DUMMY_PROCEDURE('ROLLBACK')";
                }
                // else:
                else
                {
                    s = "CALL PG_CATALOG.DUMMY_PROCEDURE('')";
                }
            }
        }
        // if not specified FROM clause - replace with pg_catalog.dual table 
        // TODO: use regex for better pattern matching
        else if (lower.startsWith("select ") && lower.indexOf("from ") == -1)
        {
            // replace NULL with cast(NULL as char(1))
            s = replaceAll(s, "NULL", "cast(NULL as char(1))");
            // replace current_schema() with current_schema:
            s = replaceAll(s, "current_schema()", "current_schema");
            s = s + " from pg_catalog.dual";
        }
        // columns discovery ODBC query:
        // TODO: ensure that this query will also work in new ODBC provider versions
        else if (lower.startsWith("select n.nspname, c.relname, a.attname, a.atttypid, t.typname, a.attnum, a.attlen, a.atttypmod, a.attnotnull,"))
        {
            // replaced query:
            String replacedQuery = "select * from pg_catalog.internal_columns_view ";

            // where clause:
            String whereClause = "";

            // if search by oid:
            Matcher m = COLUMNS_Q_SEARCH_BY_IDS_1.matcher(s);
            if (m.matches())
            {
                whereClause = " oid = " + m.group(1) + " "; 

                m = COLUMNS_Q_SEARCH_BY_IDS_1.matcher(s);
                if (m.find())
                {
                    if (whereClause != "")
                        whereClause += " and ";
                    whereClause += " ordinal_position = " + m.group(1) + " ";
                }
            }
            // else:
            else
            {
                m = COLUMNS_Q_SCHEMA.matcher(s);                
                if (m.find())
                {
                    if (whereClause != "")
                        whereClause += " and ";

                    // remove \\_ escaping:
                    whereClause += " nspname " + m.group(1) + " '" + m.group(2).toString().replace("\\\\_", "_").toUpperCase() + "'";
                }

                m = COLUMNS_Q_TABLE.matcher(s);
                if (m.find())
                {
                    if (whereClause != "")
                        whereClause += " and ";

                    // remove \\_ escaping:
                    whereClause += " relname " + m.group(1) + " '" + m.group(2).toString().replace("\\\\_", "_").toUpperCase() + "'";
                } 
            }

            if (whereClause != "")
                replacedQuery += " where " + whereClause;

            replacedQuery += " order by nspname, relname, attnum";
            s = replacedQuery;                        
        }  
        // ODBC indexes view (ignore it at the moment):
        else if (lower.startsWith("select c.relname, i.indkey, i.indisunique, i.indisclustered, a.amname, c.relhasrules, n.nspname, c.oid from pg_catalog.pg_index"))
        { 
            if (!parseStatement)
            {
                try
                {
                    // send dataset:
                    SimpleResultSet rs = new SimpleResultSet();
                    rs.addColumn("RELNAME", Types.VARCHAR, 255, 0);
                    rs.addColumn("INDKEY", Types.INTEGER, 0, 0);
                    rs.addColumn("INDISUNIQUE", Types.BOOLEAN, 0, 0);
                    rs.addColumn("INDISCLUSTERED", Types.BOOLEAN, 0, 0);
                    rs.addColumn("AMNAME", Types.VARCHAR, 0, 0);
                    rs.addColumn("RELHASRULES", Types.BOOLEAN, 0, 0);
                    rs.addColumn("NSPNAME", Types.VARCHAR, 0, 0);
                    rs.addColumn("OID", Types.INTEGER, 0, 0);

                    ResultSetMetaData meta = rs.getMetaData();
                    sendRowDescription(meta);
                    while (rs.next()) 
                    {
                       sendDataRow(rs);
                    }
                    sendCommandComplete(s, 0);
                    s = "";
                }
                catch(Exception ex) {}
            }
        }
        // ODBC tables view:
        else if (lower.startsWith("select relname, nspname, relkind from pg_catalog.pg_class c"))
        {
            String replacedQuery = "select relname, nspname, relkind from pg_catalog.internal_tables_view ";
            String whereClause = "";

            // if specified relname:
            Matcher m = TABLES_Q1.matcher(s);                
            if (m.find())
            {
                whereClause += " relname " + m.group(1) + " '" + m.group(2).toString().replace("\\\\_", "_").toUpperCase() + "'";
            }

            // if specified nspname:
            m = TABLES_Q2.matcher(s);
            if (m.find())
            {
                if (whereClause != "")
                    whereClause += " and ";

                whereClause += " nspname " + m.group(1) + " '" + m.group(2).toString().replace("\\\\_", "_").toUpperCase() + "'";
            }

            if (whereClause != "")
                replacedQuery += " where " + whereClause;

            s = replacedQuery + " order by nspname, relname";            
        }
     // Tableau driven stuff
        else if (lower.startsWith("set timezone"))
        {
        	
        	s = "CALL PG_CATALOG.DUMMY_PROCEDURE('set timezone')";
                      
        }
        // ODBC queries with E' escaping:
        else if ((lower.startsWith("select") || lower.startsWith("insert") || lower.startsWith("delete") || lower.startsWith("update")) && s.indexOf("E'") > -1)
        {            
	    //String replacedQuery = s;
	    //Matcher matcher = E_ESCAPING.matcher(s);
	    //while (matcher.find())
	    //{
	    //    // position where E' starts:
	    //    int position = matcher.start();
	    //	String tmp = matcher.group();
	    //			
	    //	// modify:
	    //	replacedQuery = replacedQuery.substring(0, position) + " " + replacedQuery.substring(position + 1); 				
	    //}			
            //s = replacedQuery;
            
            Matcher matcher = E_ESCAPING.matcher(s);
            s = matcher.replaceAll("'$1'");
        }
        // pg_type => pg_catalog.pg_type:
        else if (SELECT_PGTYPE_PATTERN.matcher(s).matches())
        {
            // if Npgsql data types request:
            if (lower.startsWith("select typname, oid from pg_type where typname"))
            {
                int idx1 = lower.indexOf("where typname");
                s = "select distinct pgtypname, oid from pg_catalog.pg_type where pgtypname" + s.substring(idx1 + 13);
            }
            else
            {
                s = SELECT_PGTYPE_PATTERN.matcher(s).replaceFirst("$1 pg_catalog.pg_type$3");                
            }
        }

        // ODBC queries type casting (::<type>):
        if ((lower.startsWith("select") || lower.startsWith("insert") || lower.startsWith("delete") || lower.startsWith("update")) && s.indexOf("'::") > -1)
        {
	    Matcher matcher2 = TYPE_CASTING.matcher(s);
            s = matcher2.replaceAll("$1");

            // time, date & timestamp casting:
            matcher2 = TYPE_CASTING_DT_1.matcher(s);
            s = matcher2.replaceAll("cast('$1' as $3)"); 
        }

        // queries type casting (::<type>), actual for Npgsql provider:
        if ((lower.startsWith("select") || lower.startsWith("insert") || lower.startsWith("delete") || lower.startsWith("update")) && s.indexOf("')::") > -1)
        {
	    Matcher matcher2 = TYPE_CASTING2.matcher(s);
            s = matcher2.replaceAll("$1");

            // time, date & timestamp casting:
            matcher2 = TYPE_CASTING_DT_2.matcher(s);
            s = matcher2.replaceAll("cast('$1' as $3)"); 
        }

        // trace:
        logger.trace("Modified query: " + s);
 
        return s;
    }

    // send command complete:
    private void sendCommandComplete(String sql, int updateCount) throws IOException 
    {
        startMessage('C');
        sql = sql.trim().toUpperCase();
        // TODO remove remarks at the beginning
        String tag;
        if (sql.startsWith("INSERT")) 
        {
            tag = "INSERT 0 " + updateCount;
        } 
        else if (sql.startsWith("DELETE")) 
        {
            tag = "DELETE " + updateCount;
        } 
        else if (sql.startsWith("UPDATE") || sql.startsWith("DEALLOCATE")) 
        {
            tag = "UPDATE " + updateCount;
        }
        // if call dummy procedure:
        else if (sql.startsWith("CALL PG_CATALOG.DUMMY_PROCEDURE('START TRANSACTION')"))
        {
            tag = "UPDATE 0";
        } 
        // if call dummy procedure:
        else if (sql.startsWith("CALL PG_CATALOG.DUMMY_PROCEDURE('COMMIT')"))
        {
            tag = "COMMIT";
        } 
        // if call dummy procedure:
        else if (sql.startsWith("CALL PG_CATALOG.DUMMY_PROCEDURE('ROLLBACK')"))
        {
            tag = "ROLLBACK";
        }
        // if call dummy procedure:
        else if (sql.startsWith("CALL PG_CATALOG.DUMMY_PROCEDURE("))
        {
            tag = "UPDATE 0";
        } 
        else if (sql.startsWith("SELECT") || sql.startsWith("CALL")) 
        {
            tag = "SELECT";
        } 
        else if (sql.startsWith("BEGIN")) 
        {
            tag = "BEGIN";
        }
        else if (sql.startsWith("END")) 
        {
            tag = "COMMIT";
        }        
        // else:
        else 
        {
            // return substring until the second space character
            int i = sql.indexOf(" ");
            if (i == -1) 
            {
                tag = sql;
            } 
            else 
            {
                int j = sql.indexOf(" ", i + 1);
                if (j == -1) 
                {
                    tag = sql.substring(0, i);
                } 
                else 
                {
                    tag = sql.substring(0, j);
                }
            }
        }
        //else 
        //{
        //    logger.trace("Check command tag: " + sql);
        //    tag = "UPDATE " + updateCount;
        //}
        writeString(tag);
        sendMessage();
    }

    // format date:
    private String formatDate(Date d, String formatPattern) 
    {
        String returnValue;
        Calendar c = GregorianCalendar.getInstance();
        c.setTime(d);
        if (c.get(Calendar.ERA) == GregorianCalendar.BC)
            formatPattern += " G";
        DateFormat format = new java.text.SimpleDateFormat(formatPattern);
        returnValue = format.format(d);
        return returnValue;
    }

    // send data row:
    private void sendDataRow(ResultSet rs) throws IOException 
    {
        try 
        {
            int columns = rs.getMetaData().getColumnCount();
            // data row:
            startMessage('D');
            // columns count:
            writeShort(columns);

            // process over columns:
            for (int i = 1; i <= columns; i++) 
            {
                String returnValue = null;                
                switch (rs.getMetaData().getColumnType(i)) 
                {
                    case Types.TINYINT:
                       returnValue = Byte.toString(rs.getByte(i));
                       if (rs.wasNull())
                           returnValue = null;
                       break;

                    case Types.SMALLINT:
                       returnValue = Short.toString(rs.getShort(i));
                       if (rs.wasNull())
                           returnValue = null;
                       break;

                    case Types.INTEGER:
                       returnValue = Integer.toString(rs.getInt(i));
                       if (rs.wasNull())
                           returnValue = null;
                       break;

                    case Types.BIGINT:
                       returnValue = Long.toString(rs.getLong(i));
                       if (rs.wasNull())
                           returnValue = null;
                       break;

                    case Types.REAL:
                       returnValue = Float.toString(rs.getFloat(i));
                       if (rs.wasNull())
                           returnValue = null;
                       break;

                    case Types.FLOAT:
                       returnValue = Double.toString(rs.getDouble(i));
                       if (rs.wasNull())
                           returnValue = null;
                       break;

                    case Types.DOUBLE:
                       returnValue = Double.toString(rs.getDouble(i));
                       if (rs.wasNull())
                           returnValue = null;
                       break;

                    case Types.DECIMAL:
                    case Types.NUMERIC:
                       BigDecimal bigDecimal = rs.getBigDecimal(i);
                       if (bigDecimal != null)
                           returnValue = bigDecimal.toPlainString();
                       break;

                    case Types.BIT:
                    case Types.BOOLEAN:
                       if ("bool".equalsIgnoreCase((rs.getMetaData().getColumnTypeName(i))) || "boolean".equalsIgnoreCase((rs.getMetaData().getColumnTypeName(i)))) 
                       {
                           Boolean b = rs.getBoolean(i);
                           returnValue = rs.wasNull() ? null : b ? "t" : "f";
			   //returnValue = rs.wasNull() ? null : b ? "1" : "0";
                       } else {
                           // TODO must return 01, 111, 10111, etc and not only true or false as now
                           returnValue = rs.getString(i);
                       }
                       break;

                    case Types.DATE:
                       java.sql.Date jdbcDate = rs.getDate(i);
                       if (jdbcDate != null) {
                           returnValue = formatDate(new Date(jdbcDate.getTime()), "yyyy-MM-dd");
                       }
                       break;

                    case Types.TIMESTAMP:
                       Timestamp ts = rs.getTimestamp(i);
                       if (ts != null) 
                       {
                          if (ts.getTime() == JDBC_DATE_INFINITY)
                              returnValue = POSTGRES_DATE_INFINITY;
                          else if (ts.getTime() == JDBC_DATE_MINUS_INFINITY)
                               returnValue = POSTGRES_DATE_MINUS_INFINITY;
                          else 
                          {
                               returnValue = formatDate(new Date(ts.getTime()), "yyyy-MM-dd HH:mm:ss");
                          }
                       }
                       break;
                     default:
                        returnValue = rs.getString(i);
                        break;
                  } 
                  		  

                  // TODO write Binary data
                  if (returnValue != null)
                  {
                     byte[] d2 = returnValue.getBytes(getEncoding());
                     writeInt(d2.length);
                     write(d2);
                  }
                  else
                  {
                     writeInt(-1); 
                  }
            }

            sendMessage();
        } 
        catch (SQLException e) 
        {
            sendErrorResponse(e);
        }
    }

    // get encoding:
    private String getEncoding() 
    {
        if ("UNICODE".equals(clientEncoding)) {
            return "UTF-8";
        }
        return clientEncoding;
    }

    // set parameter:
    private void setParameter(Prepared prep, int i, byte[] d2, int[] formatCodes) throws SQLException 
    {
        boolean text = (i >= formatCodes.length) || (formatCodes[i] == 0);
        String s;
        try 
        {               
            if (text) 
            {
                s = new String(d2, getEncoding());
            } 
            else 
            {
                logger.trace("Binary format not supported");
                s = new String(d2, getEncoding());
            }
        } 
        catch (Exception e) 
        {
            logger.error("Exception during setting parameter: " + e.toString());
            s = null;
        }

        // set value:
        prep.parameters[i] = s;
    }


    // send error response:
    private void sendErrorResponse(SQLException e) throws IOException 
    {
        logger.error("Exception: " + e.toString());
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        writeString(e.getSQLState() != null ? e.getSQLState() : "XX000");
        write('M');
        writeString(e.getMessage());
        write('D');
        writeString(e.toString());
        write(0);
        sendMessage();
    }

    // send error response:
    private void sendErrorResponseNoLog(SQLException e) throws IOException 
    {
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        writeString(e.getSQLState() != null ? e.getSQLState() : "XX000");
        write('M');
        writeString(e.getMessage());
        write('D');
        writeString(e.toString());
        write(0);
        sendMessage();
    }

    // send parameter description:
    private void sendParameterDescription(Prepared p) throws IOException 
    {
        try 
        {
            // prepare statement:
            PreparedStatement prep = conn.prepareStatement(p.sql);

            // set parameters:
            for (int i = 0; i < p.parameters.length; i++)
            {
                 prep.setString(i + 1, p.parameters[i]);
            }                                

            ParameterMetaData meta = prep.getParameterMetaData();
            int count = meta.getParameterCount();
            startMessage('t');
            writeShort(count);
            for (int i = 0; i < count; i++) 
            {
                int type;
                if (p.paramType != null && p.paramType[i] != 0) 
                {
                    type = p.paramType[i];
                } 
                else 
                {
                    type = TYPE_STRING;
                }
                checkType(type);
                writeInt(type);
            }
            sendMessage();

            prep.close();
        } 
        catch (SQLException e) 
        {
            sendErrorResponseNoLog(e);
        }
    }

    // send no data:
    private void sendNoData() throws IOException 
    {
        startMessage('n');
        sendMessage();
    }

    // send row description:
    private void sendRowDescription(ResultSetMetaData meta) throws IOException 
    {
        try 
        {
            if (meta == null) 
            {
                sendNoData();
            } 
            else 
            {
                int columns = meta.getColumnCount();
                startMessage('T');
                // number of columns:
                writeShort(columns);
                for (int i = 1; i <= columns; i++) 
                {
                    // columns name:
                    writeString(meta.getColumnLabel(i));
                    // object ID (OID):
                    writeInt(0);
                    // attribute number of the column (column index)
                    writeShort(i);
                    // data type
                    writeInt(JDBCToPostgreSQLType.getPostgreSQLType(meta.getColumnTypeName(i)));
                    
                    // attribute length:
                    switch (meta.getColumnType(i)) 
                    {
                       case Types.TINYINT:
                          writeShort(2);
                          break;

                       case Types.SMALLINT:
                          writeShort(2);
                          break;

                       case Types.INTEGER:
                          writeShort(4);
                          break;

                       case Types.BIGINT:
                          writeShort(8);
                          break;

                       case Types.REAL:
                          writeShort(4);
                          break;

                       case Types.FLOAT:
                          writeShort(8);
                          break;
     
                       case Types.DOUBLE:
                          writeShort(8);
                          break;

                       case Types.DECIMAL:
                       case Types.NUMERIC:
                          writeShort(-1);
                          break;

                       case Types.BIT:
                       case Types.BOOLEAN:
                          writeShort(1);
                          break;          	

                       case Types.DATE:
                          writeShort(4);
                          break;

                       case Types.TIME:
                       case Types.TIMESTAMP:
                          writeShort(8);
                          break;

                       //case Types.CHAR:
                       //   writeShort(meta.getColumnDisplaySize(i) + 4);
                       //   break;

                       default:
                          writeShort(-1);
                          break;
                    }                    

                    //System.out.println(meta.getColumnLabel(i) + ": " + meta.getColumnTypeName(i));

                    // pg_type.typlen                   
                    //writeShort(-1);
                    //writeShort(meta.getColumnDisplaySize(i));
                    //if (meta.getColumnTypeName(i).toUpperCase().equals("INTEGER"))
                    //{
                    //    writeShort(4);
                    //}
                    //else
                    //{
                    //    writeShort(-1);
                    //}

                    // pg_attribute.atttypmod:
                    if (meta.getColumnType(i) == Types.DECIMAL || meta.getColumnType(i) == Types.NUMERIC)
                    {
                        writeInt(meta.getPrecision(i) * 65536 + meta.getScale(i) + 4);
                    }
                    else
                    { 
                        writeInt(-1);
                    }

                    // text
                    writeShort(0);
                }
                sendMessage();
            }
        } 
        catch (SQLException e) 
        {
            sendErrorResponse(e);
        }
    }

    // get type size:
    private int getTypeSize(int type, int precision) 
    {
        switch (type) 
        {
            case Types.VARCHAR:
               return Math.max(255, precision + 10);
            default:
               return precision + 4;
        }
    }

    // send just message:
    private void sendErrorResponse(String message) throws IOException 
    {
        logger.trace("Exception: " + message);
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');
        // PROTOCOL VIOLATION
        writeString("08P01");
        write('M');
        writeString(message);
        write(0);
        sendMessage();
    }

    // send sql state & message:
    private void sendErrorResponse(String sqlState, String message) throws IOException 
    {
        logger.trace("Exception: " + message);
        startMessage('E');
        write('S');
        writeString("ERROR");
        write('C');        
        writeString(sqlState == null ? "XX000" : sqlState);
        write('M');
        writeString(message);
        write(0);
        sendMessage();
    }
               
    // send parse complete:
    private void sendParseComplete() throws IOException 
    {
        startMessage('1');
        sendMessage();
    }
    
    // set bind complete:
    private void sendBindComplete() throws IOException 
    {
        startMessage('2');
        sendMessage();
    }

    // close:
    void close() 
    {
        try 
        {
            stop = true;
            JdbcUtils.closeSilently(conn);
            if (socket != null) 
            {
                socket.close();
            }
            logger.trace("Close");
        } 
        catch (Exception e) 
        {
            logger.error(e.getStackTrace());
        }
        conn = null;
        socket = null;
        server.remove(this);
    }

    // send cleartext auth request:
    private void sendAuthenticationCleartextPassword() throws IOException 
    {
        startMessage('R');
        writeInt(3);
        sendMessage();
    }

    // send secure auth request:
    private void sendAuthenticationMD5Password() throws IOException 
    {
        startMessage('R');
        writeInt(5); // MD5
        writeStringNonNullTerminated(salt);
        sendMessage();
    }
  
    // check if specified password are equals:
    private Boolean passwordsAreEqual(String user, String plainPassword, String scrambledPassword)
    {
        MessageDigest md;
        byte[] temp_digest, pass_digest;
        byte[] hex_digest = new byte[35];
        byte[] scrambled = scrambledPassword.getBytes();

        try 
        {
            md = MessageDigest.getInstance("MD5");
            md.update(plainPassword.getBytes("US-ASCII"));
            md.update(user.getBytes("US-ASCII"));
            temp_digest = md.digest();

            bytesToHex(temp_digest, hex_digest, 0);
            md.update(hex_digest, 0, 32);
            md.update(salt.getBytes());
            pass_digest = md.digest();

            bytesToHex(pass_digest, hex_digest, 3);
            hex_digest[0] = (byte) 'm';
            hex_digest[1] = (byte) 'd';
            hex_digest[2] = (byte) '5';

            for (int i = 0; i < hex_digest.length; i++)  
            {
                if (scrambled[i] != hex_digest[i]) 
                {
                    return false;
                }
            }

        } 
        catch (Exception e) 
        {
            logger.error(e);
        }

        return true;        
    }

    // bytes to hex:
    private static void bytesToHex(byte[] bytes, byte[] hex, int offset) 
    {
        final char lookup[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

        int i, c, j, pos = offset;

        for (i = 0; i < 16; i++) 
        {
            c = bytes[i] & 0xFF;
            j = c >> 4;
            hex[pos++] = (byte) lookup[j];
            j = (c & 0xF);
            hex[pos++] = (byte) lookup[j];
        }
    }
    
    // send auth ok:
    private void sendAuthenticationOk() throws IOException 
    {
        startMessage('R');
        writeInt(0);
        sendMessage();
        sendParameterStatus("client_encoding", clientEncoding);
        sendParameterStatus("DateStyle", dateStyle);
        sendParameterStatus("integer_datetimes", "off");
        sendParameterStatus("is_superuser", "off");
        sendParameterStatus("server_encoding", clientEncoding);
        sendParameterStatus("server_version", "8.1.4");
        sendParameterStatus("session_authorization", userName);
        sendParameterStatus("standard_conforming_strings", "off");
        // TODO PostgreSQL TimeZone
        //sendParameterStatus("TimeZone", "CET");
        sendBackendKeyData();
        sendReadyForQuery();
    }

    // send ready for query:
    private void sendReadyForQuery() throws IOException 
    {
        startMessage('Z');
        char c;
        try 
        {
            if (conn.getAutoCommit()) 
            {
                // idle
                c = 'I';
            } 
            else 
            {
                // in a transaction block
                c = 'T';
            }
        } 
        catch (SQLException e) 
        {
            // failed transaction block
            c = 'E';
        }
        write((byte) c);
        sendMessage();
    }

    // send backend keydata:
    private void sendBackendKeyData() throws IOException 
    {
        startMessage('K');
        writeInt(processId);
        writeInt(secretKey);
        sendMessage();
    }

    // write string:
    private void writeString(String s) throws IOException 
    {
        write(s.getBytes(getEncoding()));
        write(0);
    }

    // write string:
    private void writeStringNonNullTerminated(String s) throws IOException 
    {
        write(s.getBytes(getEncoding()));
        //write(0);
    }

    // write int:
    private void writeInt(int i) throws IOException 
    {
        dataOut.writeInt(i);
    }

    // write short:
    private void writeShort(int i) throws IOException 
    {
        dataOut.writeShort(i);
    }

    // write binary data:
    private void write(byte[] data) throws IOException 
    {
        dataOut.write(data);
    }

    // write integer data:
    private void write(int b) throws IOException 
    {
        dataOut.write(b);
    }

    // start message:
    private void startMessage(int messageType) 
    {
        this.messageType = messageType;
        outBuffer = new ByteArrayOutputStream();
        dataOut = new DataOutputStream(outBuffer);
    }

    // send message:
    private void sendMessage() throws IOException 
    {
        dataOut.flush();
        byte[] buff = outBuffer.toByteArray();
        int len = buff.length;
        dataOut = new DataOutputStream(out);
        dataOut.write(messageType);
        dataOut.writeInt(len + 4);
        dataOut.write(buff);
        dataOut.flush();
    }

    // send parameter status:
    private void sendParameterStatus(String param, String value) throws IOException 
    {
        startMessage('S');
        writeString(param);
        writeString(value);
        sendMessage();
    }

    // set thread:
    void setThread(Thread thread) 
    {
        this.thread = thread;
    }

    // get thread:
    Thread getThread() 
    {
        return thread;
    }

    // set process id:
    void setProcessId(int id) 
    {
        this.processId = id;
    }

    /**
     * Represents a PostgreSQL Prepared object.
     */
    class Prepared 
    {
        /**
         * The object name.
         */
        String name;

        /**
         * The SQL statement.
         */
        String sql;

        /**
         * The list of parameter types (if set).
         */
        int[] paramType;

        /**
         * Parameters
         */
        String[] parameters;
    }

    /**
     * Represents a PostgreSQL Portal object.
     */
    class Portal 
    {

        /**
         * The portal name.
         */
        String name;

        /**
         * The SQL statement.
         */
        String sql;

        /**
         * The format used in the result set columns (if set).
         */
        int[] resultColumnFormat;

        /**
         * Prepared
         */
        Prepared prepared;
    }
}
