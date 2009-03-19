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
    virtual ~FennelResource()
    {
    }

    static const FennelResource &instance();
    static const FennelResource &instance(const Locale &locale);

    static void setResourceFileLocation(const std::string &location);

    /**
     * <code>sysCallFailed</code> is '<code>System call failed:  {0}</code>'
     */
    std::string sysCallFailed(const std::string &p0) const;

    /**
     * <code>duplicateKeyDetected</code> is '<code>Duplicate key detected:  {0}</code>'
     */
    std::string duplicateKeyDetected(const std::string &p0) const;

    /**
     * <code>internalError</code> is '<code>Internal error:  {0}</code>'
     */
    std::string internalError(const std::string &p0) const;

    /**
     * <code>executionAborted</code> is '<code>Execution aborted</code>'
     */
    std::string executionAborted() const;

    /**
     * <code>rowTooLong</code> is '<code>Row size ({0,number,#} bytes) exceeds maximum ({1,number,#} bytes); row data:  {2}</code>'
     */
    std::string rowTooLong(int p0, int p1, const std::string &p2) const;

    /**
     * <code>invalidParam</code> is '<code>Invalid parameter setting.  Setting must be between {0} and {1}.</code>'
     */
    std::string invalidParam(const std::string &p0, const std::string &p1) const;

    /**
     * <code>scalarQueryReturnedMultipleRows</code> is '<code>Scalar query returned more than one row</code>'
     */
    std::string scalarQueryReturnedMultipleRows() const;

    /**
     * <code>scratchMemExhausted</code> is '<code>Cache scratch memory exhausted</code>'
     */
    std::string scratchMemExhausted() const;

    /**
     * <code>uniqueConstraintViolated</code> is '<code>Unique constraint violation</code>'
     */
    std::string uniqueConstraintViolated() const;

    /**
     * <code>incompatibleDataFormat</code> is '<code>Incompatible data format encountered</code>'
     */
    std::string incompatibleDataFormat() const;

    /**
     * <code>libaioRequired</code> is '<code>Linux requires libaio package</code>'
     */
    std::string libaioRequired() const;

    /**
     * <code>unsupportedOperation</code> is '<code>Unsupported operation: {0}</code>'
     */
    std::string unsupportedOperation(const std::string &p0) const;

    /**
     * <code>outOfBackupSpace</code> is '<code>Insufficient space to execute system backup</code>'
     */
    std::string outOfBackupSpace() const;

    /**
     * <code>mismatchedRestore</code> is '<code>Commit sequence number in the restore does not match the expected value</code>'
     */
    std::string mismatchedRestore() const;

    /**
     * <code>openBackupFileFailed</code> is '<code>Open of backup file {0} failed</code>'
     */
    std::string openBackupFileFailed(const std::string &p0) const;

    /**
     * <code>readBackupFileFailed</code> is '<code>Read from backup file {0} failed</code>'
     */
    std::string readBackupFileFailed(const std::string &p0) const;

    /**
     * <code>writeBackupFileFailed</code> is '<code>Write to backup file {0} failed</code>'
     */
    std::string writeBackupFileFailed(const std::string &p0) const;

    /**
     * <code>readDataPageFailed</code> is '<code>Read of data page failed</code>'
     */
    std::string readDataPageFailed() const;

    /**
     * <code>writeDataPageFailed</code> is '<code>Write of data page failed</code>'
     */
    std::string writeDataPageFailed() const;

    /**
     * <code>outOfSpaceDuringRestore</code> is '<code>Insufficient disk space to restore backup</code>'
     */
    std::string outOfSpaceDuringRestore() const;

    /**
     * <code>bitmapEntryTooLong</code> is '<code>Bitmap entry size ({0,number,#} bytes) exceeds maximum ({1,number,#} bytes); key value:  {2}</code>'
     */
    std::string bitmapEntryTooLong(int p0, int p1, const std::string &p2) const;

    /**
     * <code>readDataFailed</code> is '<code>Could not read data file {0}</code>'
     */
    std::string readDataFailed(const std::string &p0) const;

    /**
     * <code>dataTransferFailed</code> is '<code>Could not access file {0} (size {1,number,#} bytes)</code>'
     */
    std::string dataTransferFailed(const std::string &p0, int p1) const;

    /**
     * <code>writeLogFailed</code> is '<code>Could not write log file {0}</code>'
     */
    std::string writeLogFailed(const std::string &p0) const;

    /**
     * <code>noRowsReturned</code> is '<code>Read no rows from file {0}; last error was: {1}</code>'
     */
    std::string noRowsReturned(const std::string &p0, const std::string &p1) const;

    /**
     * <code>errorsEncountered</code> is '<code>Encountered errors while processing file {0}; please see log file {1} for more information</code>'
     */
    std::string errorsEncountered(const std::string &p0, const std::string &p1) const;

    /**
     * <code>noRowDelimiter</code> is '<code>Data file {0} has no row delimiter</code>'
     */
    std::string noRowDelimiter(const std::string &p0) const;

    /**
     * <code>incompleteColumn</code> is '<code>Column has no delimiter</code>'
     */
    std::string incompleteColumn() const;

    /**
     * <code>noColumnDelimiter</code> is '<code>Row has no column delimiter</code>'
     */
    std::string noColumnDelimiter() const;

    /**
     * <code>tooFewColumns</code> is '<code>Row has too few columns</code>'
     */
    std::string tooFewColumns() const;

    /**
     * <code>tooManyColumns</code> is '<code>Row has too many columns</code>'
     */
    std::string tooManyColumns() const;

    /**
     * <code>rowTextTooLong</code> is '<code>Row text was too large</code>'
     */
    std::string rowTextTooLong() const;

    /**
     * <code>flatfileDescribeFailed</code> is '<code>Could not derive column sizes for data file {0}</code>'
     */
    std::string flatfileDescribeFailed(const std::string &p0) const;

    /**
     * <code>flatfileNoHeader</code> is '<code>Could not read header from data file {0}: {1}</code>'
     */
    std::string flatfileNoHeader(const std::string &p0, const std::string &p1) const;

    /**
     * <code>flatfileMappedRequiresLenient</code> is '<code>Flat file columns cannot be mapped without lenient mode</code>'
     */
    std::string flatfileMappedRequiresLenient() const;

    /**
     * <code>flatfileNoMappedColumns</code> is '<code>Could not map flat file columns because the flat file header {0} contained none of the target columns {1}</code>'
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
    ResourceDefinition _libaioRequired;
    ResourceDefinition _unsupportedOperation;
    ResourceDefinition _outOfBackupSpace;
    ResourceDefinition _mismatchedRestore;
    ResourceDefinition _openBackupFileFailed;
    ResourceDefinition _readBackupFileFailed;
    ResourceDefinition _writeBackupFileFailed;
    ResourceDefinition _readDataPageFailed;
    ResourceDefinition _writeDataPageFailed;
    ResourceDefinition _outOfSpaceDuringRestore;
    ResourceDefinition _bitmapEntryTooLong;
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

// End FennelResource.h
