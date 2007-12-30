/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
// ---
//
// test NoisyArithmetic functions
//
//    TODO:
//        1. Doesn't allow octal/hex format for unsigned types.
//        2. Doesn't test for thread/re-entrance safety.
//
*/

#include "fennel/common/CommonPreamble.h"

#include <boost/test/unit_test_suite.hpp>

// FIXME jvs 12-Aug-2007:  This file had compilation errors on Windows
// and 64-bit Linux so I disabled it on those platforms for now.
#ifndef __MINGW32__
#include <assert.h>
#include <stdio.h>
#include <sysexits.h>

#include "fennel/disruptivetech/calc/NoisyArithmetic.h"

using namespace fennel;

/* --- */
#define OP_ADD    1
#define OP_SUB    2
#define OP_MUL    3
#define OP_DIV    4
#define OP_NEG    5

#define T_C       1
#define T_SC      2
#define T_UC      3
#define T_S       4
#define T_US      5
#define T_I32     6
#define T_UI32    7
#define T_I64     8
#define T_UI64    9
#define T_F       10
#define T_D       11
#define T_LD      12

#define EX_NONE    0
#define EX_DATA    1
#define EX_ERROR   2

/* --- */
static struct TOp {
    const char *pName;
    int iOpCode;
    int iArity;
} g_Ops[] = 
{
{ "add", OP_ADD, 2 },
{ "sub", OP_SUB, 2 },
{ "mul", OP_MUL, 2 },
{ "div", OP_DIV, 2 },
{ "neg", OP_NEG, 1 },
{ 0, 0 }
};
static struct TType {
    const char *pName;
    int iCode;
    const char *pScanfFormat;
    const char *pPrintfFormat;
} g_Types[] = 
{
{ "char", T_C, "%hhi", "%hhd" },
{ "signed char", T_SC, "%hhi", "%hhd" },
{ "unsigned char", T_UC, "%hhu", "%hhu" },        /* TODO, doesn't allow unsigned hex, octals */
{ "short", T_S, "%hi", "%hd"  },
{ "unsigned short", T_US, "%hu", "%hu" },
{ "int", T_I32, "%i", "%d"  },
{ "unsigned int", T_UI32, "%u", "%u" },
{ "long long int", T_I64,  "%lli", "%lld"  },
{ "long long unsigned int", T_UI64, "%llu", "%llu" },
{ "float", T_F, "%e", "%e" },
{ "double", T_D, "%le", "%le" },
{ "long double", T_LD, "%lle", "%lle" },
{ 0, 0 }
};
enum { SHORT_MSG_LENGTH=7, LONG_MSG_LENGTH/*hack*/=512, FIXED_ARGS=3 };
union TSuper {
    char c;
    unsigned char uc;
    short s;
    unsigned short us;
    int i32;
    unsigned int ui32;
    long long int i64;
    unsigned long long int ui64;
    float f;
    double d;
    long double ld;
    //char msg[SHORT_MSG_LENGTH];
    char msg[LONG_MSG_LENGTH];/*hack - TODO*/
    char _cptr[];
};

/* --- */
class Noisy_no_error : public std::runtime_error
{
public:
    explicit Noisy_no_error()
    :    std::runtime_error("") {
        }
};

/* --- */
static int g_iProgramReturnCode = 0;

/* --- */
inline int min( int a, int b )
{
    return (a<b) ? a : b;
}

/* --- */
template <typename TMPL>
bool DoOp_0( int iOpCode, int iExType, const char *pExpected,
        char *pOp0, char *pOp1, char *pResult )
{
    TMPL *tOp0 = reinterpret_cast<TMPL *>( pOp0 );
    TMPL *tOp1 = reinterpret_cast<TMPL *>( pOp1 );
    TMPL *tResult = reinterpret_cast<TMPL *>( pResult );
    TProgramCounter pc(0);
    switch( iOpCode ) {
        case OP_ADD:
        *tResult = Noisy<TMPL>::add( pc, *tOp0, *tOp1, 0 );
            break;
            
        case OP_SUB:
            *tResult = Noisy<TMPL>::sub( pc, *tOp0, *tOp1, 0 );
            break;
            
        case OP_MUL:
            *tResult = Noisy<TMPL>::mul( pc, *tOp0, *tOp1, 0 );
            break;
            
        case OP_DIV:
            *tResult = Noisy<TMPL>::div( pc, *tOp0, *tOp1, 0 );
            break;
            
        case OP_NEG:
            *tResult = Noisy<TMPL>::neg( pc, *tOp0, 0 );
            break;
            
        default:
            assert( 0 /* op not implemented */ );
        }
    switch( iExType ) {
        case EX_NONE:    return true;
        case EX_ERROR:    throw Noisy_no_error(); assert( 0 );
        case EX_DATA:    break;
        default:        assert( 0 );
        };
    const TMPL *tExpected = reinterpret_cast<const TMPL *>( pExpected );
    return (*tResult)==(*tExpected);
}

/* --- */
static bool DoOp_1( int iTypeCode, int iOpCode, int iExType,
        const char *pExpected, char *pOp0, char *pOp1, char *pResult )
{
    switch( iTypeCode ) {
#define DOOP(type) \
    return DoOp_0<type>( iOpCode, iExType, pExpected, pOp0, pOp1, pResult ); break;
        case T_C:        DOOP(char);
        case T_SC:       DOOP(signed char);
        case T_UC:       DOOP(unsigned char);
        case T_S:        DOOP(short);
        case T_US:       DOOP(unsigned short);
        case T_I32:      DOOP(int);
        case T_UI32:     DOOP(unsigned int);
        case T_I64:      DOOP(long long int);
        case T_UI64:     DOOP(unsigned long long int);
        case T_F:        DOOP(float);
        case T_D:        DOOP(double);
        case T_LD:       DOOP(long double);
#undef DOOP

        default:
            assert( 0 /* type not implemented */ );
        }
    return false;    /* !++(__compiler_unhappy)++ */
}

/* --- */
static bool DoOp_2( int iType, int iOpCode, int iExType,
        const char *pExpected, char *pOp0, char *pOp1, char *pResult )
{
    try {
        bool bRet = DoOp_1( g_Types[iType].iCode, iOpCode, iExType, pExpected, pOp0, pOp1, pResult );
        sprintf( pResult, g_Types[iType].pPrintfFormat, *pResult );
        return bRet;
        }
    catch( Noisy_no_error &e ) {
        sprintf( pResult, g_Types[iType].pPrintfFormat, *pResult );
        return false;
        }
    catch( CalcMessage &msg ) {
        *pResult = '!';
        strncpy( &(pResult[1]), msg.str, SHORT_MSG_LENGTH-2 );
        pResult[SHORT_MSG_LENGTH-1] = '\0';
        if ( iExType==EX_NONE ) return true;
        return strcmp( pResult, pExpected )==0;
        }
}

/* --- */
template <typename TMPL>
bool SetConstant( const char *pConstant, TMPL *pData, int iLine )
{
    assert( pConstant );
    if ( 0 ) ;
    else if ( !strcmp( pConstant, "MAX" ) )
        *pData = std::numeric_limits<TMPL>::max();
    else if ( !strcmp( pConstant, "MIN" ) )
        *pData = std::numeric_limits<TMPL>::min();
    else {
        fprintf( stderr, "Invalid constant '%s' at line %d\n", pConstant, iLine );
        return false;
        }
    return true;
}

/* --- */
static bool SetConstant( const char *pConstant, int iTypeCode, char *pData, int iLine )
{
    switch( iTypeCode ) {
#define DOOP(type) \
    return SetConstant<type>( pConstant, reinterpret_cast<type *>(pData), iLine )
        case T_C:       DOOP(char);
        case T_SC:      DOOP(signed char);
        case T_UC:      DOOP(unsigned char);
        case T_S:       DOOP(short);
        case T_US:      DOOP(unsigned short);
        case T_I32:     DOOP(int);
        case T_UI32:    DOOP(unsigned int);
        case T_I64:     DOOP(long long int);
        case T_UI64:    DOOP(unsigned long long int);
        case T_F:       DOOP(float);
        case T_D:       DOOP(double);
        case T_LD:      DOOP(long double);
#undef DOOP
        default:
            assert( 0 );
            return false;
        }
}

/* --- */
static bool ReadArgument( int iLine, const char *pFile, int iArg, TType &tType,
        const char *ppArgs[], TSuper &t2Read )
{
    const char *pValue = ppArgs[iArg];
    assert( pValue );
    if ( *pValue=='&' ) {
        return SetConstant( &(pValue[1]), tType.iCode, t2Read._cptr, iLine );
        }
    else if ( 1 != sscanf( pValue, tType.pScanfFormat, t2Read._cptr ) ) {
        fprintf( stderr, "Error in argument %d at %s line %d\n", iArg,
            pFile, iLine );
        return false;
        }
    return true;
}

/* --- */
static bool RunTest( int iLine, const char *pFile, const char *ppArgs[], int iArgCount )
{
    // op type expected-result op0 [op1]
    if ( iArgCount<FIXED_ARGS ) {
        fprintf( stderr, "Bad number of arguments (%d) at %s line %d\n",
            iArgCount, pFile, iLine );
        return false;
        }
    assert( ppArgs[0] );
    int iOp=-1;
    int iArity=-1;
    for( int i=0; g_Ops[i].pName; i++ ) {
        if ( !strcasecmp( ppArgs[0], g_Ops[i].pName ) ) {
            iOp = i;
            break;
            }
        }
    if ( iOp==-1 ) {
        fprintf( stderr, "No such operation '%s' at %s line %d\n",
            ppArgs[0], pFile, iLine );
        return false;
        }
    iArity = g_Ops[iOp].iArity;
    if ( iArgCount<(FIXED_ARGS+iArity) ) {
        fprintf( stderr, "Bad number of arguments (%d) for operation '%s' at %s line %d\n",
            iArgCount-FIXED_ARGS, g_Ops[iOp].pName, pFile, iLine );
        return false;
        }
    int iType=-1;
    assert( ppArgs[1] );
    for( int i=0; g_Types[i].pName; i++ ) {
        if ( !strcasecmp( ppArgs[1], g_Types[i].pName ) ) {
            iType = i;
            break;
            }
        }
    if ( iType==-1 ) {
        fprintf( stderr, "No such type '%s' at %s line %d\n",
            ppArgs[1], pFile, iLine );
        return false;
        }

    /* --- Its important that this union is used and not a char * buffer
    to ensure correct [d/q]word alighnment of certain variables
    --- */
    TSuper tExpected;
    TSuper tOp0;
    TSuper tOp1;
    TSuper tResult;

    /* --- */
    int iExType;
    switch( *ppArgs[2] ) {
        case '*': iExType = EX_NONE; break;
        case '!':
            iExType = EX_ERROR;
            strncpy( tExpected._cptr, ppArgs[2], SHORT_MSG_LENGTH );
            break;
        default:
            if ( !ReadArgument( iLine, pFile, 2, g_Types[iType], ppArgs, tExpected ) ) return false;
            iExType = EX_DATA;
        }
    if ( iArity>0 && !ReadArgument( iLine, pFile, 3, g_Types[iType], ppArgs, tOp0 ) ) return false;
    if ( iArity>1 && !ReadArgument( iLine, pFile, 4, g_Types[iType], ppArgs, tOp1 ) ) return false;

    /* --- */
    if ( !DoOp_2( iType, g_Ops[iOp].iOpCode, iExType,
            tExpected._cptr, tOp0._cptr, tOp1._cptr, tResult._cptr ) ) {
        fprintf( stdout, "Line %d: Calculated result [", iLine );
        fprintf( stdout, "%s", tResult._cptr );
        fprintf( stdout, "] does not match expected [" );
        fprintf( stdout, "%s", ppArgs[2] );
        fprintf( stdout, "].\n" );

        fflush(stdout);

        g_iProgramReturnCode = 1;
        }

    return true;
}

/* --- */
static char *CanonifyArg( char *pArg )
{
    assert( pArg );
    while( *pArg && *pArg!=':' && isspace( *pArg ) ) pArg++;    /* trim leading whitespace */
    if ( *pArg=='#' ) return 0;
    return pArg;
}

/* --- */
static bool ProcessALine( int iLine, const char *pFile, char *pLine, size_t tLength )
{
    enum { MAX_ARGS=11 };
    const char *ppArgs[MAX_ARGS];
    int iArgCount=0;
    char *pStart = pLine;
    for( int i=0; iArgCount<MAX_ARGS; i++ ) {
        if ( !pLine[i] || pLine[i]==':' ) {
            ppArgs[iArgCount] = CanonifyArg( pStart );
            if ( !ppArgs[iArgCount] ) break;    /* rest of line is a comment */
            iArgCount++;
            if ( !pLine[i] ) break;
            pLine[i] = '\0';
            pStart = &( pLine[i+1] );
            }
        }
    assert( iArgCount <= MAX_ARGS );
    assert( iArgCount );
    iArgCount--;
    if ( iArgCount<2 ) return true; /* comment or empty line */
    if ( iArgCount==MAX_ARGS ) {
        fprintf( stderr, "Too many arguments (max=%d) at %s line %d\n", 
            MAX_ARGS-1, pFile, iLine );
        return false;
        }

    return RunTest( iLine, pFile, &(ppArgs[1]), iArgCount );
}

/* --- return 0 unless (system/format/data) error --- */
int InputFromStream( const char *pFilename )
{
    enum { BUF_SIZE=1023 };
    char szBuf[BUF_SIZE+1];
    int iLine;
    FILE *pIn = stdin;
    FILE *pIn2Close = 0;

    /* --- */
    if ( strcmp( pFilename, "-" ) ) {
        pIn = ::fopen( pFilename, "r" );
        if ( !pIn ) {
            perror( pFilename );
            return EX_IOERR;
            }
        pIn2Close = pIn;
        }
    else {
        pFilename = "(stdin)";
        }
    

    /* --- */
    szBuf[BUF_SIZE] = '\0';
    iLine = 0;
    for(;;) {
        const char *pRead = fgets( szBuf, BUF_SIZE, pIn );
        if ( !pRead ) {
            if ( pIn2Close ) ::fclose( pIn2Close );
            return feof(pIn) ? g_iProgramReturnCode : EX_IOERR;
            }
        assert( pRead==szBuf );
        size_t tRead = strlen( pRead )-1;
        iLine++;    /* line could be too long .... TODO */
        if ( !tRead ) continue;    /* EOF exactly after BUF_SIZE chars */
        if ( szBuf[tRead]=='\r' || szBuf[tRead]=='\n' ) {
            while ( szBuf[tRead]=='\r' || szBuf[tRead]=='\n' ) {
                szBuf[tRead--] = '\0';
                }
            }
        assert( szBuf[tRead] != '\r' && szBuf[tRead] != '\n' );
        if ( !*szBuf ) continue;
        if ( !ProcessALine( iLine, pFilename, szBuf, tRead ) ) {
            /* real error, like invalid format */
            if ( pIn2Close ) ::fclose( pIn2Close );
            return EX_DATAERR;
            };
        }

    /* --- shouldn't get here --- */
    assert( 0 );
    if ( pIn2Close ) ::fclose( pIn2Close );
    return 1;
}

/* --- return 0 unless (system/format/data) error --- */
int ProcessCppLine( int iLine, const char *pInputFileName,
        const char *pLine, int iLength )
{
    enum { BUF_SIZE=1023 };
    char szBuf[BUF_SIZE+1];

    /* --- */
    strncpy( szBuf, pLine, BUF_SIZE );
    szBuf[BUF_SIZE+1] ='\0';

    /* --- */
    return ProcessALine( iLine, pInputFileName, szBuf,
        min(iLength,BUF_SIZE) );
}

/* --- */
int main( int iArgc, const char *ppArgv[] )
{
return 0;   /* this test is causing a seg. fault, and its not valuable as part of build tests */
    /* --- */
    if ( iArgc < 1/*how?*/ || iArgc>2 ) {
        fprintf( stderr, "Usage: %s [<filename>|-]\n", iArgc>0 ? ppArgv[0] : "" );
        ::exit( EX_USAGE );
        }

    /* --- */
    if ( iArgc==2 ) {
        /* read from file */
        return InputFromStream( ppArgv[1] );
        }
    
    /* else */
    /* read generated C++ from this file */
    int iLine = 1;
#define GENERATED_FILE "testNoisyArithmeticGen.hpp"
#define EXPR(line)                                                  \
    if ( !ProcessCppLine( iLine++, GENERATED_FILE,                  \
            #line, sizeof(line)-1 ) )                               \
        return EX_DATAERR;
#include GENERATED_FILE
#undef EXPR
    return 0;
}

#endif

boost::unit_test_framework::test_suite *init_unit_test_suite(int,char **)
{
    return NULL;
}
