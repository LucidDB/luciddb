/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
*/
package com.lucidera.luciddb.applib;

import java.sql.Date;
import java.sql.SQLException;
import java.util.Calendar;


/**
 * Class for date conversions
 * Ported from //bb/bb713/server/SQL/BBDate.java
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public class BBDate
{
    private static final int MAX_CHAR_LEN = 32;

    private char[]		m_caIn		= new char[ MAX_CHAR_LEN ];
    private char[]		m_caMask	= new char[ MAX_CHAR_LEN ];
    private int			m_iMaskLen	= 0;

    private int			m_iYear;
    private int			m_iMonth;
    private int			m_iDate;
    private char[]		m_caMonth	= new char[ 3 ];

    /**
     * Convert a string to a date, using a specified date format mask.
     * @param in String to convert
     * @param mask Format mask for the string
     * @return Date datatype
     * @exception SQLException thrown when the mask is invalid, or the string
     *     is invalid for the specified mask.
     */
    public Date toDate( String in, String mask ) throws SQLException
    {
        if( ( in == null ) || ( mask == null ) )
            return null;
        else
        {
            standardize( in, mask );
            return new Date( m_iYear, m_iMonth, m_iDate );
        }
    }

    /**
     * Standardize a date string to yyyy-mm-dd format, given a date mask
     * @param in String to convert
     * @param mask Format mask for the string
     * @exception SQLException thrown when the mask is invalid, or the string
     *     is invalid for the specified mask.
     */
    private void standardize( String in, String mask ) throws SQLException
    {
        ApplibResource res = ApplibResourceObject.get();
        int inLen = in.length();

        if( inLen > MAX_CHAR_LEN ) {
            inLen = MAX_CHAR_LEN;
        }

        // An instance of this UDF class is instantiated for the query;
        // initialize the mask upon instantiation to improve performance
        // when multiple records are processed.
        // NOTE: this would break if we reused instances across queries.
        if( m_iMaskLen == 0 ) {
            m_iMaskLen = mask.length();
            if( m_iMaskLen > MAX_CHAR_LEN ) {
                m_iMaskLen = MAX_CHAR_LEN;
            }
            mask.toUpperCase().getChars( 0, m_iMaskLen, m_caMask, 0 );
        }

        // TODO: Improve performance here by eliminating toUpperCase() ?
        in.toUpperCase().getChars( 0, inLen, m_caIn, 0 );

        // Strip trailing spaces (faster than trim())
        while( m_caIn[ inLen-1 ] == ' ' ) {
            inLen--;
        }

        // Maximum number of allowed digits
        int yearDigits = 4;
        int monthDigits = 2;
        int dayDigits = 2;

        // Initialize date values, with year being 1900-based,
        // month being 0-based, and day being 1-based.
        // Note that this affects the arithmetic later.
        m_iYear = 0;		// default year: 1900
        m_iMonth = 0;		// default month: January
        m_iDate = 1;		// default date: 1

        for( int i=m_iMaskLen-1; i>=0; i-- ) {
            switch( m_caMask[ i ] ) {
            case 'Y':
                switch( --yearDigits ) {
                case 3:
                    m_iYear += toDigit( m_caIn[ i ] );
                    break;
                case 2:
                    m_iYear += toDigit( m_caIn[ i ] ) * 10;
                    break;
                case 1:
                    m_iYear += ( toDigit( m_caIn[ i ] ) - 9 ) * 100;
                    break;
                case 0:
                    m_iYear += ( toDigit( m_caIn[ i ] ) - 1 ) * 1000;
                    break;
                default:
                    throw new SQLException(res.InvalidYearFmtMask.str());
                }
                break;
            case 'M':
                switch( --monthDigits ) {
                case 1:
                    m_iMonth += toDigit( m_caIn[ i ] ) - 1;
                    break;
                case 0:
                    m_iMonth += toDigit( m_caIn[ i ] ) * 10;
                    break;
                default:
                    throw new SQLException(res.InvalidMonthFmtMask.str());
                }
                break;
            case 'D':
                switch( --dayDigits ) {
                case 1:
                    m_iDate += toDigit( m_caIn[ i ] ) - 1;
                    break;
                case 0:
                    m_iDate += toDigit( m_caIn[ i ] ) * 10;
                    break;
                default:
                    throw new SQLException(res.InvalidDayFmtMask.str());
                }
                break;
            case 'N':
                // better have M-O-N
                if( i < 2 ) {
                    break;
                }
                m_caMonth[ 2 ] = m_caIn[ i ];
                if( m_caMask[ i-1 ] != 'O' ) {
                    break;
                }
                m_caMonth[ 1 ] = m_caIn[ --i ];
                if( m_caMask[ i-1 ] != 'M' ) {
                    break;
                }
                m_caMonth[ 0 ] = m_caIn[ --i ];
                if( --monthDigits < 0 ) {
                    throw new SQLException(res.InvalidMonthFmtMask.str());
                }
                monthDigits = 0;
                // Ugly, but faster than string comparisons in Java
                if( m_caMonth[ 0 ] == 'J' && m_caMonth[ 1 ] == 'A' 
                    && m_caMonth[ 2 ] == 'N' )  // JAN
                {
                    m_iMonth = 0;
                } else if( m_caMonth[ 0 ] == 'F' && m_caMonth[ 1 ] == 'E' 
                    && m_caMonth[ 2 ] == 'B' )	// FEB
                {
                    m_iMonth = 1;
                } else if( m_caMonth[ 0 ] == 'M' && m_caMonth[ 1 ] == 'A' 
                    && m_caMonth[ 2 ] == 'R' )	// MAR
                {
                    m_iMonth = 2;
                } else if( m_caMonth[ 0 ] == 'A' && m_caMonth[ 1 ] == 'P' 
                    && m_caMonth[ 2 ] == 'R' )	// APR
                {
                    m_iMonth = 3;
                } else if( m_caMonth[ 0 ] == 'M' && m_caMonth[ 1 ] == 'A' 
                    && m_caMonth[ 2 ] == 'Y' )	// MAY
                {
                    m_iMonth = 4;
                } else if( m_caMonth[ 0 ] == 'J' && m_caMonth[ 1 ] == 'U' 
                    && m_caMonth[ 2 ] == 'N' )	// JUN
                {
                    m_iMonth = 5;
                } else if( m_caMonth[ 0 ] == 'J' && m_caMonth[ 1 ] == 'U' 
                    && m_caMonth[ 2 ] == 'L' )	// JUL
                {
                    m_iMonth = 6;
                } else if( m_caMonth[ 0 ] == 'A' && m_caMonth[ 1 ] == 'U'
                    && m_caMonth[ 2 ] == 'G' )	// AUG
                {
                    m_iMonth = 7;
                } else if( m_caMonth[ 0 ] == 'S' && m_caMonth[ 1 ] == 'E' 
                    && m_caMonth[ 2 ] == 'P' )	// SEP
                {
                    m_iMonth = 8;
                } else if( m_caMonth[ 0 ] == 'O' && m_caMonth[ 1 ] == 'C' 
                    && m_caMonth[ 2 ] == 'T' )	// OCT
                {
                    m_iMonth = 9;
                } else if( m_caMonth[ 0 ] == 'N' && m_caMonth[ 1 ] == 'O' 
                    && m_caMonth[ 2 ] == 'V' )	// NOV
                {
                    m_iMonth = 10;
                } else if( m_caMonth[ 0 ] == 'D' && m_caMonth[ 1 ] == 'E' 
                    && m_caMonth[ 2 ] == 'C' )	// DEC
                {
                    m_iMonth = 11;
                } else {
                    throw new SQLException(res.InvalidMonthInputString.str());
                }
                break;
            default:
                break;
            }
        }
    }

    private int toDigit( char in ) throws SQLException
    {
        int ret;

        if( in == ' ' ) {
            ret = 0;
        } else {
            ret = in - '0';
        }

        if( ( ret < 0 ) || ( ret > 9 ) ) {
            throw new SQLException(
                ApplibResourceObject.get().InvalidDigitInputString.str());
        }

        return ret;
    }
}

// End BBDate.java
