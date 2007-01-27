/*
// $Id$
// (C) Copyright 2007-2007 LucidEra, Inc.
*/
package com.lucidera.luciddb.mbean.sysviews;

import java.sql.*;
import java.util.*;
import javax.management.openmbean.*;

public interface SessionsMBean
{
    public TabularData getSessions() throws Exception;
    public String printSessions() throws Exception;
}

// End SessionsMBean.java
