/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#ifndef Fennel_MappedPageListener_Included
#define Fennel_MappedPageListener_Included

FENNEL_BEGIN_NAMESPACE

class CachePage;

/**
 * MappedPageListener defines an interface which allows a derived class
 * to receive notifications of events on mapped pages.  All methods have dummy
 * implementations (not pure virtual) so derived classes only need to override
 * those of interest.
 */
class FENNEL_CACHE_EXPORT MappedPageListener
{
public:
    virtual ~MappedPageListener();

    /**
     * Receives notification from CacheImpl as soon as a page is mapped, before
     * any I/O is initiated to retrieve the page contents.
     * Called with the page mutex held, so the implementation must
     * take care to avoid deadlock.
     *
     * @param page the page being mapped
     */
    virtual void notifyPageMap(CachePage &page);

    /**
     * Receives notification from CacheImpl just before a page is unmapped.
     * Called with the page mutex held, so the implementation must take care to
     * avoid deadlock.
     *
     * @param page the page being unmapped
     */
    virtual void notifyPageUnmap(CachePage &page);

    /**
     * Receives notification from CacheImpl after a page read completes.
     * Called with the page mutex held, so the implementation must take care to
     * avoid deadlock.
     *
     * @param page the page read
     */
    virtual void notifyAfterPageRead(CachePage &page);

    /**
     * Receives notification from CacheImpl the first time a page becomes dirty
     * after it has been mapped (but before the contents have changed).
     * Allows some logging action to be taken; for example, making
     * a backup copy of the unmodified page contents.  Note that when
     * called for a newly allocated page, the page contents are invalid.
     * Because it is implied that the calling thread already has an exclusive
     * lock on the page, no cache locks are held when called.
     *
     * @param page the page being modified
     *
     * @param bDataValid if true, the page data was already valid; if false,
     * the data was invalid, but has now been marked valid since it's about to
     * be written
     */
    virtual void notifyPageDirty(CachePage &page,bool bDataValid);

    /**
     * Informs CacheImpl whether a dirty page can safely be flushed to disk.
     * Called with the page mutex held, so the implementation must take care to
     * avoid deadlock.
     *
     * @param page the page to be flushed
     */
    virtual bool canFlushPage(CachePage &page);

    /**
     * Receives notification from CacheImpl just before a dirty page is flushed
     * to disk.  Allows some logging action to be taken; for example, flushing
     * corresponding write-ahead log pages, or storing a checksum in the page
     * header.  Called with the page mutex held, so the implementation must
     * take care to avoid deadlock.
     *
     * @param page the page to be flushed
     */
    virtual void notifyBeforePageFlush(CachePage &page);

    /**
     * Receives notification from CacheImpl when a page flush completes
     * successfully.  Called with the page mutex held, so the implementation
     * must take care to avoid deadlock.
     *
     * @param page the page that was flushed
     */
    virtual void notifyAfterPageFlush(CachePage &page);

    /**
     * Receives notification that a page has been flushed during a checkpoint.
     * Also determines if the listener on the page needs to be reset.
     *
     * <p>Note that if the page listener is reset, that page may not be unmapped
     * during a CHECKPOINT_FLUSH_AND_UNMAP checkpoint call.
     *
     * <p>This method should be called immediately after the page flush has
     * completed while the checkpoint is still in progress.
     *
     * @param page the page that was flushed
     *
     * @return NULL if the listener on the page does not need to be reset;
     * otherwise, returns the listener that the page should be reset to
     */
    virtual MappedPageListener *notifyAfterPageCheckpointFlush(CachePage &page);

    /**
     * Retrieves the tracing wrapper corresponding to this listener if
     * tracing is turned on.  Otherwise, returns this listener itself.
     *
     * @return tracing segment corresponding to a listener
     */
    virtual MappedPageListener *getTracingListener() = 0;
};

FENNEL_END_NAMESPACE

#endif

// End MappedPageListener.h
