// This class is generated. Do NOT modify it, or
// add it to source control.

/**
 * This class was generated
 * by class mondrian.resource.ResourceGen
 * from /home/jvs/open/dev/fennel/common/FennelResource.xml
 * on Fri Oct 29 13:24:17 PDT 2004.
 * It contains a list of messages, and methods to
 * retrieve and format those messages.
 **/

#ifndef Fennel_FennelResource_Included
#define Fennel_FennelResource_Included

#include <ctime>
#include <string>

#include "Locale.h"
#include "ResourceDefinition.h"
#include "ResourceBundle.h"

// begin includes specified by /home/jvs/open/dev/fennel/common/FennelResource.xml
// end includes specified by /home/jvs/open/dev/fennel/common/FennelResource.xml

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

	/** <code>sysCallFailed</code> is 'System call failed:  {0}'	 */
    std::string sysCallFailed(const std::string &p0) const;

	/** <code>duplicateKeyDetected</code> is 'Duplicate key detected:  {0}'	 */
    std::string duplicateKeyDetected(const std::string &p0) const;

	/** <code>internalError</code> is 'Internal error:  {0}'	 */
    std::string internalError(const std::string &p0) const;

	/** <code>executionAborted</code> is 'Execution aborted'	 */
    std::string executionAborted() const;

    private:
    ResourceDefinition _sysCallFailed;
    ResourceDefinition _duplicateKeyDetected;
    ResourceDefinition _internalError;
    ResourceDefinition _executionAborted;

    template<class _GRB, class _BC, class _BC_ITER>
        friend _GRB *makeInstance(_BC &bundleCache, const Locale &locale);
};

} // end namespace fennel

#endif // Fennel_FennelResource_Included
