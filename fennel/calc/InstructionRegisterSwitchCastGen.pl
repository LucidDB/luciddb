#!/usr/bin/perl

# from fennel/tuple/StandardDescriptor.h:

$STANDARD_TYPE_INT_8 = 1;
$STANDARD_TYPE_UINT_8 = 2;
$STANDARD_TYPE_INT_16 = 3;
$STANDARD_TYPE_UINT_16 = 4;
$STANDARD_TYPE_INT_32 = 5;
$STANDARD_TYPE_UINT_32 = 6;
$STANDARD_TYPE_INT_64 = 7;
$STANDARD_TYPE_UINT_64 = 8;
$STANDARD_TYPE_BOOL = 9;
$STANDARD_TYPE_REAL = 10;
$STANDARD_TYPE_DOUBLE = 11;
$STANDARD_TYPE_CHAR = 12;
$STANDARD_TYPE_VARCHAR = 13;
$STANDARD_TYPE_BINARY = 14;
$STANDARD_TYPE_VARBINARY = 15;

# Note: SQL99 6.22 Syntax Rule 6 shows that booleans are not castable
# to/from numbers.

# Note: String casting is handled by a set of extended instructions.

%std = (
        $STANDARD_TYPE_INT_8, "STANDARD_TYPE_INT_8",
        $STANDARD_TYPE_UINT_8, "STANDARD_TYPE_UINT_8",
        $STANDARD_TYPE_INT_16, "STANDARD_TYPE_INT_16",
        $STANDARD_TYPE_UINT_16, "STANDARD_TYPE_UINT_16",
        $STANDARD_TYPE_INT_32, "STANDARD_TYPE_INT_32",
        $STANDARD_TYPE_UINT_32, "STANDARD_TYPE_UINT_32",
        $STANDARD_TYPE_INT_64, "STANDARD_TYPE_INT_64",
        $STANDARD_TYPE_UINT_64, "STANDARD_TYPE_UINT_64",
#       $STANDARD_TYPE_BOOL, "STANDARD_TYPE_BOOL",
        $STANDARD_TYPE_REAL, "STANDARD_TYPE_REAL",
        $STANDARD_TYPE_DOUBLE, "STANDARD_TYPE_DOUBLE",
#       $STANDARD_TYPE_CHAR, "STANDARD_TYPE_CHAR",
#       $STANDARD_TYPE_VARCHAR, "STANDARD_TYPE_VARCHAR",
#       $STANDARD_TYPE_BINARY, "STANDARD_TYPE_BINARY",
#       $STANDARD_TYPE_VARBINARY, "STANDARD_TYPE_VARBINARY",
        );

%ctype = (
          $STANDARD_TYPE_INT_8, "int8_t",
          $STANDARD_TYPE_UINT_8, "uint8_t",
          $STANDARD_TYPE_INT_16, "int16_t",
          $STANDARD_TYPE_UINT_16, "uint16_t",
          $STANDARD_TYPE_INT_32, "int32_t",
          $STANDARD_TYPE_UINT_32, "uint32_t",
          $STANDARD_TYPE_INT_64, "int64_t",
          $STANDARD_TYPE_UINT_64, "uint64_t",
#         $STANDARD_TYPE_BOOL, "bool",
          $STANDARD_TYPE_REAL, "float",
          $STANDARD_TYPE_DOUBLE, "double",
#         $STANDARD_TYPE_CHAR, "char *",
#         $STANDARD_TYPE_VARCHAR, "char *",
#         $STANDARD_TYPE_BINARY, "char *",
#         $STANDARD_TYPE_VARBINARY, "char *",
          );

$first = 1;

print "// InstructionRegsiterSwitchCast.h\n\n";

for ($t1 = $STANDARD_TYPE_INT_8; $t1 <= $STANDARD_TYPE_DOUBLE; $t1++) {
    if ($t1 == $STANDARD_TYPE_BOOL) {
        $t1++;
    }
    for ($t2 = $STANDARD_TYPE_INT_8; $t2 <= $STANDARD_TYPE_DOUBLE; $t2++) {
        if ($t2 == $STANDARD_TYPE_BOOL) {
            $t2++;
        }
        if ($first) {
            $first = 0;
        } else {
            print "else ";
        }
        print "if (type1 == ", $std{$t1}, " && type2 == ", $std{$t2}, ") {\n";
        print "    InstructionRegister::registerInstance2<";
        print $ctype{$t1}, ", ", $ctype{$t2}, ", INSTCLASS2>\n";
        print "        (type1, type2);\n";
        print "} ";
    }
}
print "else {\n";
print '    throw std::logic_error("InstructionRegisterSwitchCast.h");';
print "\n}\n\n";
print "// InstructionRegisterSwitchCast.h END\n";
