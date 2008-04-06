-- $Id$
-- Test queries with different identifiers

-- !set verbose true

-- Test normal identifiers
create table sales.idtest (
    obj_count integer primary key,
    "Desc" varchar(40)
);

!columns sales.idtest

!columns IDTEST

insert into sales.idtest
values ( 15, 'lamps' ),
       ( 20, 'cameras' ),
       ( 44, 'frogs' );

select Obj_Count, "Desc" from sales.idtest;

drop table sales.idtest;

-- Test identifiers with spaces
create schema "identifier test schema?";
set schema '"identifier test schema?"';

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
create table "Reallylong`1234567890-=~!@#$%^&*()_+qwertyuiop[]\asdfghjkl;'zxcvbnm,./QWERTYUIOP{}|ASDFGHJKL:ZXCVBNM<>?tablenamewithpunctuation." (
    "longcolumnnamewithnofunnycharactersabcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefgABCDEFG0123456" integer primary key);

insert into "Reallylong`1234567890-=~!@#$%^&*()_+qwertyuiop[]\asdfghjkl;'zxcvbnm,./QWERTYUIOP{}|ASDFGHJKL:ZXCVBNM<>?tablenamewithpunctuation."
values 54, 89, 32;

!outputformat csv
select * from "Reallylong`1234567890-=~!@#$%^&*()_+qwertyuiop[]\asdfghjkl;'zxcvbnm,./QWERTYUIOP{}|ASDFGHJKL:ZXCVBNM<>?tablenamewithpunctuation.";

-- fails: table identifier too long
create table "Reallylong`1234567890-=~!@#$%^&*()_+qwertyuiop[]\asdfghjkl;'zxcvbnm,./QWERTYUIOP{}|ASDFGHJKL:ZXCVBNM<>?tablenamewithpunctuation..." (
  "col" integer primary key);

-- fails: column identifier too long
create table "longcoltab" (
  "longcolumnnamewithnofunnycharactersabcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefgABCDEFG01234567" integer primary key);

-- fails: schema identifier too long
create schema "longschemaname_mahnamahna_quitelongschemaname_mahnamahna_verylongschemaname_mahnamahna_veryveryverylongschemaname_mahnamahna_yeah";
