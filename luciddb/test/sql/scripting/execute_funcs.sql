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
