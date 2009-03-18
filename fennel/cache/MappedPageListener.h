/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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
class MappedPageListener
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
