-- function execution can be used to save the user from having to
-- modify an existing program to call itself.

values(applib.execute_function('js', 'function execute() {'
'    return "I love cookies.";'
'  }'));
values(applib.execute_function('js', 'function foo() {'
'    var fn = function() {'
'      return "I really love cookies with functions inside.";'
'    };'
'    return fn();'
'  }', 'foo'));
values(applib.execute_method('js', 'var myObj = {'
'    "C" : "o",'
'    "o" : "k",'
'    "i" : "e",'
'    "s" : "!",'
'    "love" : function() {'
'      return this.s;'
'    }'
'  }', 'myObj', 'love'));

-- test one-arg func
values(applib.execute_function('js', 'function echo(arg) { return arg; }',
    'echo', 'Hello World'));

-- test two-arg func
create schema regex;
create table regex.countries(name varchar(128));
insert into regex.countries
values
('Uruguay'),
('Paraguay'),
('Chile'),
('Argentina'),
('Venezuela');

select * from regex.countries where cast(
  applib.execute_function('js',
    'function execute(input, pattern) {
      return new RegExp(pattern).test(input)
    }',
    'execute',
    name,
    '(A|U).*')
  AS boolean);

drop schema regex cascade;

