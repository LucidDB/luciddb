/*
// $Id$
// (C) Copyright 2007-2007 LucidEra, Inc.
*/
package com.lucidera.luciddb.mbean.server;

import java.sql.*;
import java.util.*;

public interface PingServerMBean
{
    public String getCurrentStatus() throws Exception;
    public String getInfo();
}

// End PingServerMBean.java
