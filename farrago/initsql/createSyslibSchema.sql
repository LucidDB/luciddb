-- $Id$
-- This script creates the syslib schema, to expose the system routines in
-- package net.sf.farrago.syslib.

!set verbose true

create schema syslib;
set schema 'syslib';

-- lets an administrator kill an executing statement
create procedure kill_statement(in id bigint)
  language java
  parameter style java
  no sql
  external name 'class net.sf.farrago.syslib.FarragoKillUDR.killStatement';

-- lets an administrator kill a running session
create procedure kill_session(in id bigint)
  language java
  parameter style java
  no sql
  external name 'class net.sf.farrago.syslib.FarragoKillUDR.killSession';
