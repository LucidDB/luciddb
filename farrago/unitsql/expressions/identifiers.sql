-- $Id$
-- Test queries with different identifiers

-- !set verbose true

-- Test normal identifiers
create table sales.idtest (
    count integer primary key,
    "Desc" varchar(40)
);

!columns sales.idtest

!columns IDTEST

insert into sales.idtest
values ( 15, 'lamps' ),
       ( 20, 'cameras' ),
       ( 44, 'frogs' );

select Count, "Desc" from sales.idtest;

drop table sales.idtest;

-- Test identifiers with spaces
create schema "identifier test schema?";
set schema "identifier test schema?";

create table "My Table" (
    "id" integer primary key,
    "col with spaces" varchar(40),
    "%#$@!#%$_.,=+()" integer,
    "import" boolean
);

-- hmm, !columns does like this
!columns "My Table"

insert into "My Table" values
    ( 1, 'row one', 55, true),
    ( 2, 'row two', 67, false);
   
select * from "My Table";
 
select "col with spaces", "%#$@!#%$_.,=+()" from "identifier test schema?"."My Table";

select "id" + "%#$@!#%$_.,=+()", "import" from "My Table";

select "My * IDs"."id" 
from (select "id" from "My Table") "My * IDs";

-- Test cases where an error is reported 
select "COL WITH SPACES" from "My Table";

-- Test identifiers with non-ASCII characters
create table "non-ASCII table \u00bc\uc2bb\u00c3\u00bb" (
  "normal" integer primary key, 
  "column \uc234\uc2bd\uc3bc\uc380\uc2b4\uc2bb\uc386\uc2b9\uc388\uc391" integer
);

insert into "non-ASCII table \u00bc\uc2bb\u00c3\u00bb"
values (6, 7) , (8, 9);

select "normal" from "non-ASCII table \u00bc\uc2bb\u00c3\u00bb";
select "column \uc234\uc2bd\uc3bc\uc380\uc2b4\uc2bb\uc386\uc2b9\uc388\uc391"
    as "column non-ASCII"
    from "non-ASCII table \u00bc\uc2bb\u00c3\u00bb";

-- Test long identifiers
create table "Reallyreallyreallyreallylong`1234567890-=~!@#$%^&*()_+qwertyuiop[]\asdfghjkl;'zxcvbnm,./QWERTYUIOP{}|ASDFGHJKL:ZXCVBNM<>?tablenamewithmanyfunnycharacters" (
    "veryveryverylongcolumnnamewithnofunnycharactersjustalphabeticalandnumbersabcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxyz1234567890" integer primary key);

insert into "Reallyreallyreallyreallylong`1234567890-=~!@#$%^&*()_+qwertyuiop[]\asdfghjkl;'zxcvbnm,./QWERTYUIOP{}|ASDFGHJKL:ZXCVBNM<>?tablenamewithmanyfunnycharacters"
values 54, 89, 32;

select * from "Reallyreallyreallyreallylong`1234567890-=~!@#$%^&*()_+qwertyuiop[]\asdfghjkl;'zxcvbnm,./QWERTYUIOP{}|ASDFGHJKL:ZXCVBNM<>?tablenamewithmanyfunnycharacters";
