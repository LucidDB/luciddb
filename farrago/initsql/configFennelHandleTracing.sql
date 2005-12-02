-- $Id$
-- This configures Farrago to write a Fennel JNI handle trace to a file
-- fennel.handles in the working directory in which Farrago is started.

alter system set "jniHandleTraceFile" = 'fennel.handles';

