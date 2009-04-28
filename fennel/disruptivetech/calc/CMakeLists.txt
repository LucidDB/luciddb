# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

INCLUDE(${CMAKE_CURRENT_SOURCE_DIR}/FindParsers.cmake)

IF(PARSERS_FOUND)

  GENERATE_BISON_FLEX_SOURCES(${CMAKE_CURRENT_SOURCE_DIR}/CalcGrammar.ypp ""
    ${CMAKE_CURRENT_SOURCE_DIR}/CalcLexer.lpp   "")

  INCLUDE_DIRECTORIES(
    ${CMAKE_CURRENT_BINARY_DIR}
    )
ELSE(PARSERS_FOUND)
  # Opps, didn't find the parsers
  MESSAGE(SEND_ERROR "Oops, flex and/or bison can't be found.")
ENDIF(PARSERS_FOUND)


add_library(
calc STATIC
${BF_SOURCES}
BoolInstruction.cpp
Calculator.cpp
ExtDateTime.cpp
ExtendedInstruction.cpp
ExtendedInstructionTable.cpp
JumpInstruction.cpp
RegisterReference.cpp
CalcGrammar.ypp
CalcLexer.lpp
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
ExtWinAggFuncs.cpp
NoisyArithmetic.cpp
)
