/*
// $Id$
// (C) Copyright 2007-2007 LucidEra, Inc.
*/
package com.lucidera.luciddb.mbean.sysviews;

import java.sql.*;
import java.util.*;
import javax.management.openmbean.*;

public interface PerfCountersMBean
{
    public TabularData getPerfCounters() throws Exception;
    public String printPerfCounters() throws Exception;
}

// End PerfCountersMBean.java
