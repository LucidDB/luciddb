// InstructionRegsiterSwitchCast.h

if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_INT_8) {
    InstructionRegister::registerInstance2<int8_t, int8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_UINT_8) {
    InstructionRegister::registerInstance2<int8_t, uint8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_INT_16) {
    InstructionRegister::registerInstance2<int8_t, int16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_UINT_16) {
    InstructionRegister::registerInstance2<int8_t, uint16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_INT_32) {
    InstructionRegister::registerInstance2<int8_t, int32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_UINT_32) {
    InstructionRegister::registerInstance2<int8_t, uint32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_INT_64) {
    InstructionRegister::registerInstance2<int8_t, int64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_UINT_64) {
    InstructionRegister::registerInstance2<int8_t, uint64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_REAL) {
    InstructionRegister::registerInstance2<int8_t, float, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_8 && type2 == STANDARD_TYPE_DOUBLE) {
    InstructionRegister::registerInstance2<int8_t, double, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_INT_8) {
    InstructionRegister::registerInstance2<uint8_t, int8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_UINT_8) {
    InstructionRegister::registerInstance2<uint8_t, uint8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_INT_16) {
    InstructionRegister::registerInstance2<uint8_t, int16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_UINT_16) {
    InstructionRegister::registerInstance2<uint8_t, uint16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_INT_32) {
    InstructionRegister::registerInstance2<uint8_t, int32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_UINT_32) {
    InstructionRegister::registerInstance2<uint8_t, uint32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_INT_64) {
    InstructionRegister::registerInstance2<uint8_t, int64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_UINT_64) {
    InstructionRegister::registerInstance2<uint8_t, uint64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_REAL) {
    InstructionRegister::registerInstance2<uint8_t, float, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_8 && type2 == STANDARD_TYPE_DOUBLE) {
    InstructionRegister::registerInstance2<uint8_t, double, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_INT_8) {
    InstructionRegister::registerInstance2<int16_t, int8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_UINT_8) {
    InstructionRegister::registerInstance2<int16_t, uint8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_INT_16) {
    InstructionRegister::registerInstance2<int16_t, int16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_UINT_16) {
    InstructionRegister::registerInstance2<int16_t, uint16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_INT_32) {
    InstructionRegister::registerInstance2<int16_t, int32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_UINT_32) {
    InstructionRegister::registerInstance2<int16_t, uint32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_INT_64) {
    InstructionRegister::registerInstance2<int16_t, int64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_UINT_64) {
    InstructionRegister::registerInstance2<int16_t, uint64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_REAL) {
    InstructionRegister::registerInstance2<int16_t, float, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_16 && type2 == STANDARD_TYPE_DOUBLE) {
    InstructionRegister::registerInstance2<int16_t, double, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_16 && type2 == STANDARD_TYPE_INT_8) {
    InstructionRegister::registerInstance2<uint16_t, int8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_16 && type2 == STANDARD_TYPE_UINT_8) {
    InstructionRegister::registerInstance2<uint16_t, uint8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_16 && type2 == STANDARD_TYPE_INT_16) {
    InstructionRegister::registerInstance2<uint16_t, int16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_16 && type2 == STANDARD_TYPE_UINT_16) {
    InstructionRegister::registerInstance2<uint16_t, uint16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_16 && type2 == STANDARD_TYPE_INT_32) {
    InstructionRegister::registerInstance2<uint16_t, int32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_16 && type2 == STANDARD_TYPE_UINT_32) {
    InstructionRegister::registerInstance2<uint16_t, uint32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_16 && type2 == STANDARD_TYPE_INT_64) {
    InstructionRegister::registerInstance2<uint16_t, int64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_16 && type2 == STANDARD_TYPE_UINT_64) {
    InstructionRegister::registerInstance2<uint16_t, uint64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_16 && type2 == STANDARD_TYPE_REAL) {
    InstructionRegister::registerInstance2<uint16_t, float, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_16 && type2 == STANDARD_TYPE_DOUBLE) {
    InstructionRegister::registerInstance2<uint16_t, double, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_INT_8) {
    InstructionRegister::registerInstance2<int32_t, int8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_UINT_8) {
    InstructionRegister::registerInstance2<int32_t, uint8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_INT_16) {
    InstructionRegister::registerInstance2<int32_t, int16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_UINT_16) {
    InstructionRegister::registerInstance2<int32_t, uint16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_INT_32) {
    InstructionRegister::registerInstance2<int32_t, int32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_UINT_32) {
    InstructionRegister::registerInstance2<int32_t, uint32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_INT_64) {
    InstructionRegister::registerInstance2<int32_t, int64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_UINT_64) {
    InstructionRegister::registerInstance2<int32_t, uint64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_REAL) {
    InstructionRegister::registerInstance2<int32_t, float, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_32 && type2 == STANDARD_TYPE_DOUBLE) {
    InstructionRegister::registerInstance2<int32_t, double, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_32 && type2 == STANDARD_TYPE_INT_8) {
    InstructionRegister::registerInstance2<uint32_t, int8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_32 && type2 == STANDARD_TYPE_UINT_8) {
    InstructionRegister::registerInstance2<uint32_t, uint8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_32 && type2 == STANDARD_TYPE_INT_16) {
    InstructionRegister::registerInstance2<uint32_t, int16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_32 && type2 == STANDARD_TYPE_UINT_16) {
    InstructionRegister::registerInstance2<uint32_t, uint16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_32 && type2 == STANDARD_TYPE_INT_32) {
    InstructionRegister::registerInstance2<uint32_t, int32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_32 && type2 == STANDARD_TYPE_UINT_32) {
    InstructionRegister::registerInstance2<uint32_t, uint32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_32 && type2 == STANDARD_TYPE_INT_64) {
    InstructionRegister::registerInstance2<uint32_t, int64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_32 && type2 == STANDARD_TYPE_UINT_64) {
    InstructionRegister::registerInstance2<uint32_t, uint64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_32 && type2 == STANDARD_TYPE_REAL) {
    InstructionRegister::registerInstance2<uint32_t, float, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_32 && type2 == STANDARD_TYPE_DOUBLE) {
    InstructionRegister::registerInstance2<uint32_t, double, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_INT_8) {
    InstructionRegister::registerInstance2<int64_t, int8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_UINT_8) {
    InstructionRegister::registerInstance2<int64_t, uint8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_INT_16) {
    InstructionRegister::registerInstance2<int64_t, int16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_UINT_16) {
    InstructionRegister::registerInstance2<int64_t, uint16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_INT_32) {
    InstructionRegister::registerInstance2<int64_t, int32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_UINT_32) {
    InstructionRegister::registerInstance2<int64_t, uint32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_INT_64) {
    InstructionRegister::registerInstance2<int64_t, int64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_UINT_64) {
    InstructionRegister::registerInstance2<int64_t, uint64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_REAL) {
    InstructionRegister::registerInstance2<int64_t, float, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_INT_64 && type2 == STANDARD_TYPE_DOUBLE) {
    InstructionRegister::registerInstance2<int64_t, double, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_64 && type2 == STANDARD_TYPE_INT_8) {
    InstructionRegister::registerInstance2<uint64_t, int8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_64 && type2 == STANDARD_TYPE_UINT_8) {
    InstructionRegister::registerInstance2<uint64_t, uint8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_64 && type2 == STANDARD_TYPE_INT_16) {
    InstructionRegister::registerInstance2<uint64_t, int16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_64 && type2 == STANDARD_TYPE_UINT_16) {
    InstructionRegister::registerInstance2<uint64_t, uint16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_64 && type2 == STANDARD_TYPE_INT_32) {
    InstructionRegister::registerInstance2<uint64_t, int32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_64 && type2 == STANDARD_TYPE_UINT_32) {
    InstructionRegister::registerInstance2<uint64_t, uint32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_64 && type2 == STANDARD_TYPE_INT_64) {
    InstructionRegister::registerInstance2<uint64_t, int64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_64 && type2 == STANDARD_TYPE_UINT_64) {
    InstructionRegister::registerInstance2<uint64_t, uint64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_64 && type2 == STANDARD_TYPE_REAL) {
    InstructionRegister::registerInstance2<uint64_t, float, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_UINT_64 && type2 == STANDARD_TYPE_DOUBLE) {
    InstructionRegister::registerInstance2<uint64_t, double, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_INT_8) {
    InstructionRegister::registerInstance2<float, int8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_UINT_8) {
    InstructionRegister::registerInstance2<float, uint8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_INT_16) {
    InstructionRegister::registerInstance2<float, int16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_UINT_16) {
    InstructionRegister::registerInstance2<float, uint16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_INT_32) {
    InstructionRegister::registerInstance2<float, int32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_UINT_32) {
    InstructionRegister::registerInstance2<float, uint32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_INT_64) {
    InstructionRegister::registerInstance2<float, int64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_UINT_64) {
    InstructionRegister::registerInstance2<float, uint64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_REAL) {
    InstructionRegister::registerInstance2<float, float, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_REAL && type2 == STANDARD_TYPE_DOUBLE) {
    InstructionRegister::registerInstance2<float, double, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_INT_8) {
    InstructionRegister::registerInstance2<double, int8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_UINT_8) {
    InstructionRegister::registerInstance2<double, uint8_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_INT_16) {
    InstructionRegister::registerInstance2<double, int16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_UINT_16) {
    InstructionRegister::registerInstance2<double, uint16_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_INT_32) {
    InstructionRegister::registerInstance2<double, int32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_UINT_32) {
    InstructionRegister::registerInstance2<double, uint32_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_INT_64) {
    InstructionRegister::registerInstance2<double, int64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_UINT_64) {
    InstructionRegister::registerInstance2<double, uint64_t, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_REAL) {
    InstructionRegister::registerInstance2<double, float, INSTCLASS2>
        (type1, type2);
} else if (type1 == STANDARD_TYPE_DOUBLE && type2 == STANDARD_TYPE_DOUBLE) {
    InstructionRegister::registerInstance2<double, double, INSTCLASS2>
        (type1, type2);
} else {
    throw std::logic_error("InstructionRegisterSwitchCast.h");
}

// InstructionRegisterSwitchCast.h END
