-- $Id$
-- Tests for Firewater distributed storage DDL

create server embedded_node
foreign data wrapper sys_firewater_embedded_wrapper
options (user_name 'sa');

create partition p1 on (embedded_node)
description 'First partition on embedded_node';

create partition p2 on (embedded_node)
description 'Second partition on embedded_node';

-- should fail:  not a firewater server
create partition p3 on (sys_mock_foreign_data)
description 'Bad partition';

-- should fail:  duplicate name
create partition p1 on (sys_firewater_embedded_server)
description 'First partition on embedded_node';

-- should fail:  unknown node name
create partition p100 on (imaginary_node)
description 'First partition on embedded_node';

-- should fail:  restricted by presence of partition
drop server embedded_node;

-- should fail:  ditto
drop server embedded_node restrict;

-- should fail:  CASCADE isn't actually supported, despite the prev msg :P
drop server embedded_node cascade;

drop partition p1;

drop partition p2;

-- should succeed now
drop server embedded_node;
