/*
// $Id$
// (C) Copyright 2007-2007 LucidEra, Inc.
*/
package com.lucidera.luciddb.mbean.sysviews;

import java.sql.*;
import java.util.*;
import javax.management.openmbean.*;

public interface SqlStatementsMBean
{
    public TabularData getStatements() throws Exception;
    public String printStatements() throws Exception;
    public String getSqlStatements(String sessionId) throws Exception;
    public String getDetailedSqlInfo(String stmtId) throws Exception;
}

// End SqlStatementsMBean.java
