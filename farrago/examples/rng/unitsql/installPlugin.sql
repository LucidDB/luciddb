-- $Id$
-- Test installation of RNG plugin

set schema 'sys_boot.sys_boot';

create jar rngplugin 
library 'file:${FARRAGO_HOME}/examples/rng/plugin/FarragoRng.jar' 
options(0);

-- NOTE:  this will shut down the system, so don't add any commands
-- after it
alter system add catalog jar rngplugin;
