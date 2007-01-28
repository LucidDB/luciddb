/*
// $Id$
// (C) Copyright 2007-2007 LucidEra, Inc.
*/
package com.lucidera.luciddb.mbean.sysviews;

import java.sql.*;
import java.util.*;
import javax.management.openmbean.*;

public interface ForeignSourcesMBean
{
    public TabularData getForeignServers() throws Exception;
    public TabularData getForeignServerOptions() throws Exception;
    public TabularData getForeignWrappers() throws Exception;
    public TabularData getForeignWrapperOptions() throws Exception;

    public String printForeignServers() throws Exception;
    public String printForeignServerOptions() throws Exception;
    public String printForeignWrappers() throws Exception;
    public String printForeignWrapperOptions() throws Exception;
}

// End ForeignSourcesMBean.java
