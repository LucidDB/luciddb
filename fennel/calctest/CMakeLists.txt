# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

# FIXME:  tuple name clashes with lib; can we get rid of it?
# FIXME:  testNoisyArithmetic

set(PROGRAMS
testCalc
testCalcExtended
CalcAssemblerTest
CalcInstFactoryTest
CalcCastTest
CalcMiscTest
CalcExtRegExpTest
CalcExtStringTest
CalcExtContextTest
CalcExtDateTimeTest
CalcExtMathTest
CalcExtCastTest
CalcExtDynamicVariableTest
CalcExtWinAggFuncTest
)

set(SUITE_PROGRAMS
CalcExecStreamTest
)

set(STRINGBUFFER_PROGRAMS
SqlRegExpTest
SqlStringTest
SqlDateTest
SqlStringAsciiTest
)

set(IMPORTED_LIBS
  fenneltest
  fennel_calculator
  fennel_ftrs
  fennel_db
  fennel_btree
  fennel_exec
  fennel_tuple
  fennel_txn
  fennel_segment
  fennel_cache
  fennel_device
  fennel_common
)

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.cpp)
  target_link_libraries(${program}
    ${STD_LIBS}
    ${IMPORTED_LIBS})
  add_test(${program} ${program})
endforeach(program)

foreach(program ${SUITE_PROGRAMS})
  add_executable(${program} ${program}.cpp ${program}Suite.cpp)
  target_link_libraries(${program}
    ${STD_LIBS}
    ${IMPORTED_LIBS})
  add_test(${program} ${program})
endforeach(program)

foreach(program ${STRINGBUFFER_PROGRAMS})
  add_executable(${program} ${program}.cpp SqlStringBuffer.cpp)
  target_link_libraries(${program}
    ${STD_LIBS}
    ${IMPORTED_LIBS})
  add_test(${program} ${program})
endforeach(program)
