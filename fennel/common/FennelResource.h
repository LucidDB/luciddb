// This class is generated. Do NOT modify it manually.

/**
 * This class was generated
 * by class org.eigenbase.resgen.ResourceGen
 * from .../FennelResource.xml
 * It contains a list of messages, and methods to
 * retrieve and format those messages.
 */

#ifndef Fennel_FennelResource_Included
#define Fennel_FennelResource_Included

#include <ctime>
#include <string>

#include "Locale.h"
#include "ResourceDefinition.h"
#include "ResourceBundle.h"

// begin includes specified by .../FennelResource.xml
// end includes specified by .../FennelResource.xml

namespace fennel {


class FennelResource;
typedef map<Locale, FennelResource*> FennelResourceBundleCache;

class FennelResource : ResourceBundle
{
    protected:
    explicit FennelResource(Locale locale);

    public:
    virtual ~FennelResource() { }

    static const FennelResource &instance();
    static const FennelResource &instance(const Locale &locale);

    static void setResourceFileLocation(const std::string &location);

    /**
     * <code>sysCallFailed</code> is 'System call failed:  {0}'
     */
    std::string sysCallFailed(const std::string &p0) const;

    /**
     * <code>duplicateKeyDetected</code> is 'Duplicate key detected:  {0}'
     */
    std::string duplicateKeyDetected(const std::string &p0) const;

    /**
     * <code>internalError</code> is 'Internal error:  {0}'
     */
    std::string internalError(const std::string &p0) const;

    /**
     * <code>executionAborted</code> is 'Execution aborted'
     */
    std::string executionAborted() const;

    /**
     * <code>rowTooLong</code> is 'Row size ({0,number,#} bytes) exceeds maximum ({1,number,#} bytes); row data:  {2}'
     */
    std::string rowTooLong(int p0, int p1, const std::string &p2) const;

    /**
     * <code>invalidParam</code> is 'Invalid parameter setting.  Setting must be between {0} and {1}.'
     */
    std::string invalidParam(const std::string &p0, const std::string &p1) const;

    /**
     * <code>scalarQueryReturnedMultipleRows</code> is 'Scalar query returned more than one row'
     */
    std::string scalarQueryReturnedMultipleRows() const;

    /**
     * <code>scratchMemExhausted</code> is 'Cache scratch memory exhausted'
     */
    std::string scratchMemExhausted() const;

    /**
     * <code>uniqueConstraintViolated</code> is 'Unique constraint violation'
     */
    std::string uniqueConstraintViolated() const;

    /**
     * <code>incompatibleDataFormat</code> is 'Incompatible data format encountered'
     */
    std::string incompatibleDataFormat() const;

    /**
     * <code>cacheAllocFailed</code> is 'Cache memory allocation failed because ''{0}'''
     */
    std::string cacheAllocFailed(const std::string &p0) const;

    /**
     * <code>readDataFailed</code> is 'Could not read data file {0}'
     */
    std::string readDataFailed(const std::string &p0) const;

    /**
     * <code>dataTransferFailed</code> is 'Could not access file {0} (size {1,number,#} bytes)'
     */
    std::string dataTransferFailed(const std::string &p0, int p1) const;

    /**
     * <code>writeLogFailed</code> is 'Could not write log file {0}'
     */
    std::string writeLogFailed(const std::string &p0) const;

    /**
     * <code>noRowsReturned</code> is 'Read no rows from file {0}; last error was: {1}'
     */
    std::string noRowsReturned(const std::string &p0, const std::string &p1) const;

    /**
     * <code>errorsEncountered</code> is 'Encountered errors while processing file {0}; please see log file {1} for more information'
     */
    std::string errorsEncountered(const std::string &p0, const std::string &p1) const;

    /**
     * <code>noRowDelimiter</code> is 'Data file {0} has no row delimiter'
     */
    std::string noRowDelimiter(const std::string &p0) const;

    /**
     * <code>incompleteColumn</code> is 'Column has no delimiter'
     */
    std::string incompleteColumn() const;

    /**
     * <code>noColumnDelimiter</code> is 'Row has no column delimiter'
     */
    std::string noColumnDelimiter() const;

    /**
     * <code>tooFewColumns</code> is 'Row has too few columns'
     */
    std::string tooFewColumns() const;

    /**
     * <code>tooManyColumns</code> is 'Row has too many columns'
     */
    std::string tooManyColumns() const;

    /**
     * <code>rowTextTooLong</code> is 'Row text was too large'
     */
    std::string rowTextTooLong() const;

    /**
     * <code>flatfileDescribeFailed</code> is 'Could not derive column sizes for data file {0}'
     */
    std::string flatfileDescribeFailed(const std::string &p0) const;

    /**
     * <code>flatfileNoHeader</code> is 'Could not read header from data file {0}: {1}'
     */
    std::string flatfileNoHeader(const std::string &p0, const std::string &p1) const;

    /**
     * <code>flatfileMappedRequiresLenient</code> is 'Flat file columns cannot be mapped without lenient mode'
     */
    std::string flatfileMappedRequiresLenient() const;

    /**
     * <code>flatfileNoMappedColumns</code> is 'Could not map flat file columns because the flat file header {0} contained none of the target columns {1}'
     */
    std::string flatfileNoMappedColumns(const std::string &p0, const std::string &p1) const;

    private:
    ResourceDefinition _sysCallFailed;
    ResourceDefinition _duplicateKeyDetected;
    ResourceDefinition _internalError;
    ResourceDefinition _executionAborted;
    ResourceDefinition _rowTooLong;
    ResourceDefinition _invalidParam;
    ResourceDefinition _scalarQueryReturnedMultipleRows;
    ResourceDefinition _scratchMemExhausted;
    ResourceDefinition _uniqueConstraintViolated;
    ResourceDefinition _incompatibleDataFormat;
    ResourceDefinition _cacheAllocFailed;
    ResourceDefinition _readDataFailed;
    ResourceDefinition _dataTransferFailed;
    ResourceDefinition _writeLogFailed;
    ResourceDefinition _noRowsReturned;
    ResourceDefinition _errorsEncountered;
    ResourceDefinition _noRowDelimiter;
    ResourceDefinition _incompleteColumn;
    ResourceDefinition _noColumnDelimiter;
    ResourceDefinition _tooFewColumns;
    ResourceDefinition _tooManyColumns;
    ResourceDefinition _rowTextTooLong;
    ResourceDefinition _flatfileDescribeFailed;
    ResourceDefinition _flatfileNoHeader;
    ResourceDefinition _flatfileMappedRequiresLenient;
    ResourceDefinition _flatfileNoMappedColumns;

    template<class _GRB, class _BC, class _BC_ITER>
        friend _GRB *makeInstance(_BC &bundleCache, const Locale &locale);
};

} // end namespace fennel

#endif // Fennel_FennelResource_Included
