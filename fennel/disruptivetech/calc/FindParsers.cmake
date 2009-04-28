# $Id$

# /*
#   For more information, please see: http://software.sci.utah.edu
#   The MIT License
#   Copyright (c) 2005-2006
#   Scientific Computing and Imaging Institute, University of Utah
#   License for the specific language governing rights and limitations under
#   Permission is hereby granted, free of charge, to any person obtaining a
#   copy of this software and associated documentation files (the "Software"),
#   to deal in the Software without restriction, including without limitation
#   the rights to use, copy, modify, merge, publish, distribute, sublicense,
#   and/or sell copies of the Software, and to permit persons to whom the
#   Software is furnished to do so, subject to the following conditions:
#   The above copyright notice and this permission notice shall be included
#   in all copies or substantial portions of the Software.
#   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
#   OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
#   THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#   FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#   DEALINGS IN THE SOFTWARE.
# */

SET(PARSERS_FOUND FOOBAR)

# These variables need to be specified in order to get CMake not to
# barf on the IF(EXISTS ${BISON_EXECUTABLE} ..) expression even though
# the code shouldn't get called.  By setting them to BISON_EXECUTABLE

SET(BISON_EXECUTABLE "BISON_EXECUTABLE-NOTFOUND" CACHE FILEPATH "bison
executable")
SET(FLEX_EXECUTABLE "FLEX_EXECUTABLE-NOTFOUND" CACHE FILEPATH "flex
executable")
# Mark these variables as advanced options
MARK_AS_ADVANCED(FORCE BISON_EXECUTABLE)
MARK_AS_ADVANCED(FORCE FLEX_EXECUTABLE)

# You need at least version 2.4 for this to work.
IF("${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}" LESS 2.4)
  MESSAGE("You need at least version 2.4 for generating flex and bison
parsers.  Go get it from http://www.cmake.org/HTML/Download.html";)
  SET(PARSERS_FOUND 0)

ELSE("${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}" LESS 2.4)

  FIND_PROGRAM(BISON_EXECUTABLE
    NAMES bison
    PATHS ${BISON_DIR} )

  FIND_PROGRAM(FLEX_EXECUTABLE
    NAMES flex
    PATHS ${FLEX_DIR} )

  IF(EXISTS ${BISON_EXECUTABLE} AND EXISTS ${FLEX_EXECUTABLE})
    SET(PARSERS_FOUND 1)

  ELSE(EXISTS ${BISON_EXECUTABLE} AND EXISTS ${FLEX_EXECUTABLE})
    SET(PARSERS_FOUND 0)
    # Print some error messages to the user
    IF (NOT EXISTS ${BISON_EXECUTABLE})
      MESSAGE("Couldn't find bison executable.  Please check value in
BISON_EXECUTABLE in advanced settings.")
    ENDIF (NOT EXISTS ${BISON_EXECUTABLE})
    IF (NOT EXISTS ${FLEX_EXECUTABLE})
      MESSAGE("Couldn't find flex executable.  Please check value in
FLEX_EXECUTABLE in advanced settings.")
    ENDIF (NOT EXISTS ${FLEX_EXECUTABLE})

  ENDIF(EXISTS ${BISON_EXECUTABLE} AND EXISTS ${FLEX_EXECUTABLE})

ENDIF("${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}" LESS 2.4)

# These are helper functions for parsers.

# parser is the parser file name like parser.y
# lexer is like lexer.l

# The names of the output files will be based on the input names.
# BF_SOURCES will be parser.cc, parser.h and lexer.cc.

MACRO(GENERATE_BISON_FLEX_SOURCES parser parser_args
                                  lexer  lexer_args)
  GET_FILENAME_COMPONENT(parser_base "${parser}" NAME_WE)

  SET(BISON_TAB_C "${CMAKE_CURRENT_BINARY_DIR}/${parser_base}.tab.cpp")
  SET(BISON_TAB_H "${CMAKE_CURRENT_BINARY_DIR}/${parser_base}.tab.hpp")
  SET(BISON_CC    "${CMAKE_CURRENT_BINARY_DIR}/${parser_base}.cpp")
  SET(BISON_H     "${CMAKE_CURRENT_BINARY_DIR}/${parser_base}.h")

  ADD_CUSTOM_COMMAND(
    OUTPUT ${BISON_TAB_C} ${BISON_TAB_H}
    COMMAND ${BISON_EXECUTABLE}
    ARGS "${parser}" ${parser_args} "--defines"
    DEPENDS "${parser}"
    COMMENT "Generating ${BISON_TAB_C} ${BISON_TAB_H} from ${parser}"
    )

  ADD_CUSTOM_COMMAND(
    OUTPUT ${BISON_CC}
    COMMAND      ${CMAKE_COMMAND}
    ARGS -E copy ${BISON_TAB_C} ${BISON_CC}
    DEPENDS ${BISON_TAB_C}
    COMMENT "Copying ${BISON_TAB_C} to ${BISON_CC}"
    )

  ADD_CUSTOM_COMMAND(
    OUTPUT ${BISON_H}
    COMMAND      ${CMAKE_COMMAND}
    ARGS -E copy ${BISON_TAB_H} ${BISON_H}
    DEPENDS ${BISON_TAB_H}
    COMMENT "Copying ${BISON_TAB_H} to ${BISON_H}"
    )

  GET_FILENAME_COMPONENT(lexer_base "${lexer}" NAME_WE)
  SET(FLEX_C "${CMAKE_CURRENT_BINARY_DIR}/lex.yy.c")
  SET(FLEX_CC "${CMAKE_CURRENT_BINARY_DIR}/${lexer_base}.cc")

  ADD_CUSTOM_COMMAND(
    OUTPUT ${FLEX_C}
    COMMAND ${FLEX_EXECUTABLE}
    ARGS "${lexer}" ${lexer_args}
    DEPENDS "${lexer}" ${BISON_H}
    COMMENT "Generating ${FLEX_C} from ${lexer}"
    )

  ADD_CUSTOM_COMMAND(
    OUTPUT ${FLEX_CC}
    COMMAND      ${CMAKE_COMMAND}
    ARGS -E copy ${FLEX_C} ${FLEX_CC}
    DEPENDS ${FLEX_C}
    COMMENT "Copying ${FLEX_C} to ${FLEX_CC}"
    )

  SET(BF_SOURCES ${BISON_CC} ${BISON_H} ${FLEX_CC})

ENDMACRO(GENERATE_BISON_FLEX_SOURCES)
