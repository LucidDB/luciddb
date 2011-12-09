# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

set_directory_properties(PROPERTIES ADDITIONAL_MAKE_CLEAN_FILES CalcLexer.cpp)

INCLUDE(${CMAKE_CURRENT_SOURCE_DIR}/FindParsers.cmake)

IF(PARSERS_FOUND)

  GENERATE_BISON_FLEX_SOURCES(CalcGrammar.ypp ""
    CalcLexer.lpp   "")

  INCLUDE_DIRECTORIES(
    ${CMAKE_CURRENT_BINARY_DIR}
    )
ELSE(PARSERS_FOUND)
  # Opps, didn't find the parsers
  MESSAGE(SEND_ERROR "Oops, flex and/or bison can't be found.")
ENDIF(PARSERS_FOUND)

add_definitions(-DFENNEL_CALCULATOR_EXPORTS)

add_library(
calcparser STATIC
${BF_SOURCES}
CalcGrammar.ypp
CalcLexer.lpp)

add_library(
calculator STATIC
BoolInstruction.cpp
Calculator.cpp
ExtDateTime.cpp
ExtendedInstruction.cpp
ExtendedInstructionTable.cpp
JumpInstruction.cpp
RegisterReference.cpp
CalcAssembler.cpp
InstructionFactory.cpp
InstructionSignature.cpp
CalcInit.cpp
SqlDate.cpp
SqlString.cpp
SqlRegExp.cpp
SqlStringAscii.cpp
ExtString.cpp
ExtCast.cpp
ExtRegExp.cpp
StringToHex.cpp
ExtMath.cpp
ExtDynamicVariable.cpp
WinAggHistogramStrA.cpp
WinDistinctStrA.cpp
ExtWinAggFuncs.cpp
NoisyArithmetic.cpp
CalcExecStream.cpp
CalcExcn.cpp
SqlState.cpp
)
