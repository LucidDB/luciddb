// This class is generated. Do NOT modify it manually.

/**
 * This class was generated
 * by class org.eigenbase.resgen.ResourceGen
 * from .../FennelResource.xml
 * It contains a list of messages, and methods to
 * retrieve and format those messages.
 */

// begin common include specified by .../FennelResource.xml
#include "CommonPreamble.h"
// end common include specified by .../FennelResource.xml
#include "FennelResource.h"
#include "ResourceBundle.h"
#include "Locale.h"

#include <map>
#include <string>

namespace fennel {

using namespace std;

#define BASENAME ("FennelResource")

static FennelResourceBundleCache bundleCache;
static string bundleLocation("");

const FennelResource &FennelResource::instance()
{
    return FennelResource::instance(Locale::getDefault());
}

const FennelResource &FennelResource::instance(const Locale &locale)
{
    return *makeInstance<FennelResource, FennelResourceBundleCache, FennelResourceBundleCache::iterator>(bundleCache, locale);
}

void FennelResource::setResourceFileLocation(const string &location)
{
    bundleLocation = location;
}

FennelResource::FennelResource(Locale locale)
    : ResourceBundle(BASENAME, locale, bundleLocation),
      _sysCallFailed(this, "sysCallFailed"),
      _duplicateKeyDetected(this, "duplicateKeyDetected"),
      _internalError(this, "internalError"),
      _executionAborted(this, "executionAborted"),
      _rowTooLong(this, "rowTooLong"),
      _invalidParam(this, "invalidParam"),
      _scalarQueryReturnedMultipleRows(this, "scalarQueryReturnedMultipleRows"),
      _scratchMemExhausted(this, "scratchMemExhausted"),
      _uniqueConstraintViolated(this, "uniqueConstraintViolated"),
      _incompatibleDataFormat(this, "incompatibleDataFormat"),
      _readDataFailed(this, "readDataFailed"),
      _dataTransferFailed(this, "dataTransferFailed"),
      _writeLogFailed(this, "writeLogFailed"),
      _noRowsReturned(this, "noRowsReturned"),
      _errorsEncountered(this, "errorsEncountered"),
      _noRowDelimiter(this, "noRowDelimiter"),
      _incompleteColumn(this, "incompleteColumn"),
      _noColumnDelimiter(this, "noColumnDelimiter"),
      _tooFewColumns(this, "tooFewColumns"),
      _tooManyColumns(this, "tooManyColumns"),
      _rowTextTooLong(this, "rowTextTooLong"),
      _flatfileDescribeFailed(this, "flatfileDescribeFailed"),
      _flatfileNoHeader(this, "flatfileNoHeader"),
      _flatfileMappedRequiresLenient(this, "flatfileMappedRequiresLenient"),
      _flatfileNoMappedColumns(this, "flatfileNoMappedColumns")
{ }

string FennelResource::sysCallFailed(const std::string &p0) const
{
    return _sysCallFailed.format(p0);
}
string FennelResource::duplicateKeyDetected(const std::string &p0) const
{
    return _duplicateKeyDetected.format(p0);
}
string FennelResource::internalError(const std::string &p0) const
{
    return _internalError.format(p0);
}
string FennelResource::executionAborted() const
{
    return _executionAborted.format();
}
string FennelResource::rowTooLong(int p0, int p1, const std::string &p2) const
{
    return _rowTooLong.format(p0, p1, p2);
}
string FennelResource::invalidParam(const std::string &p0, const std::string &p1) const
{
    return _invalidParam.format(p0, p1);
}
string FennelResource::scalarQueryReturnedMultipleRows() const
{
    return _scalarQueryReturnedMultipleRows.format();
}
string FennelResource::scratchMemExhausted() const
{
    return _scratchMemExhausted.format();
}
string FennelResource::uniqueConstraintViolated() const
{
    return _uniqueConstraintViolated.format();
}
string FennelResource::incompatibleDataFormat() const
{
    return _incompatibleDataFormat.format();
}
string FennelResource::readDataFailed(const std::string &p0) const
{
    return _readDataFailed.format(p0);
}
string FennelResource::dataTransferFailed(const std::string &p0, int p1) const
{
    return _dataTransferFailed.format(p0, p1);
}
string FennelResource::writeLogFailed(const std::string &p0) const
{
    return _writeLogFailed.format(p0);
}
string FennelResource::noRowsReturned(const std::string &p0, const std::string &p1) const
{
    return _noRowsReturned.format(p0, p1);
}
string FennelResource::errorsEncountered(const std::string &p0, const std::string &p1) const
{
    return _errorsEncountered.format(p0, p1);
}
string FennelResource::noRowDelimiter(const std::string &p0) const
{
    return _noRowDelimiter.format(p0);
}
string FennelResource::incompleteColumn() const
{
    return _incompleteColumn.format();
}
string FennelResource::noColumnDelimiter() const
{
    return _noColumnDelimiter.format();
}
string FennelResource::tooFewColumns() const
{
    return _tooFewColumns.format();
}
string FennelResource::tooManyColumns() const
{
    return _tooManyColumns.format();
}
string FennelResource::rowTextTooLong() const
{
    return _rowTextTooLong.format();
}
string FennelResource::flatfileDescribeFailed(const std::string &p0) const
{
    return _flatfileDescribeFailed.format(p0);
}
string FennelResource::flatfileNoHeader(const std::string &p0, const std::string &p1) const
{
    return _flatfileNoHeader.format(p0, p1);
}
string FennelResource::flatfileMappedRequiresLenient() const
{
    return _flatfileMappedRequiresLenient.format();
}
string FennelResource::flatfileNoMappedColumns(const std::string &p0, const std::string &p1) const
{
    return _flatfileNoMappedColumns.format(p0, p1);
}

} // end namespace fennel
