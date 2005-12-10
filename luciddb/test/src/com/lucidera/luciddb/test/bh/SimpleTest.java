/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.luciddb.test.bh;

import org.apache.beehive.test.tools.tch.compose.AutoTest;
import org.apache.beehive.test.tools.tch.compose.TestContext;

/**
 * Simple "hello world" test for bh-base test in the luciddb area
 *
 * @author boris
 * @version $Id$
 */

public class SimpleTest
  extends AutoTest {
  
  public SimpleTest(TestContext tc) {
    super(tc);
  } 

  public boolean testFirst() 
  {
      inform("First method");
      return success("No problem");
  }

  public boolean testThird() 
  {
      require("Second");
      inform("Third method");
      return success("No problem");
  }

  public boolean testSecond() 
  {
      require("First");
      inform("Second method");
      return success("No problem");
  }

  public boolean testForth() 
  {
      require("Third");
      inform("Forth method");
      return success("No problem");
  }
  
}


// End SimpleTest.java
