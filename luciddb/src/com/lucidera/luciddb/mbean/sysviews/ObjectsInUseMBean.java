/*
// $Id$
// (C) Copyright 2007-2007 LucidEra, Inc.
*/
package com.lucidera.luciddb.mbean.sysviews;

import java.sql.*;
import java.util.*;
import javax.management.openmbean.*;

public interface ObjectsInUseMBean
{
    public TabularData getObjectsInUse() throws Exception;
    public String printObjectsInUse() throws Exception;
}

// End ObjectsInUseMBean.java
