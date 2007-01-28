/*
// $Id$
// (C) Copyright 2007-2007 LucidEra, Inc.
*/
package com.lucidera.luciddb.mbean.sysviews;

import java.sql.*;
import java.util.*;
import javax.management.openmbean.*;

public interface SystemParametersMBean
{
    public TabularData getSystemParameters() throws Exception;
    public String printSystemParameters() throws Exception;
//     public void alterSystem(String name, String value) throws Exception;
}

// End SystemParametersMBean.java
