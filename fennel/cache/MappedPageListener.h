/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
     * Called by CacheImpl as soon as a page is mapped, before any I/O is
     * initiated to retrieve the page contents.  This notification method is
     * called with the page mutex held, so the implementation must take care to
     * avoid deadlock.
     *
     * @param page the page being mapped
     */
    virtual void notifyPageMap(CachePage &page);
    
    /**
     * Called by CacheImpl just before a page is unmapped.  This notification
     * method is called with the page mutex held, so the implementation must
     * take care to avoid deadlock.
     *
     * @param page the page being unmapped
     */
    virtual void notifyPageUnmap(CachePage &page);
    
    /**
     * Called by CacheImpl after a page read completes.  This notification
     * method is called with the page mutex held, so the implementation must
     * take care to avoid deadlock.
     *
     * @param page the page read
     */
    virtual void notifyAfterPageRead(CachePage &page);
    
    /**
     * Called by CacheImpl the first time a page becomes dirty after it has
     * been mapped (but before the contents have changed).  This notification
     * allows some logging action to be taken; for example, making a backup
     * copy of the unmodified page contents.  Note that when this is called for
     * a newly allocated page, the page contents are invalid.  Because it is
     * implied that the calling thread already has an exclusive lock on the
     * page, no cache locks are held when this notification is called.
     *
     * @param page the page being modified
     *
     * @param bDataValid if true, the page data was already valid; if false,
     * the data was invalid, but has now been marked valid since it's about to
     * be written
     */
    virtual void notifyPageDirty(CachePage &page,bool bDataValid);

    /**
     * Called by CacheImpl to determine whether a dirty page can safely be
     * flushed to disk.  This notification method is called with the page
     * mutex held, so the implementation must take care to avoid deadlock.
     *
     * @param page the page to be flushed
     */
    virtual bool canFlushPage(CachePage &page);
    
    /**
     * Called by CacheImpl just before a dirty page is flushed to disk.  This
     * notification allows some logging action to be taken; for example,
     * flushing corresponding write-ahead log pages, or storing a checksum in
     * the page header.  This notification method is called with the page
     * mutex held, so the implementation must take care to avoid deadlock.
     *
     * @param page the page to be flushed
     */
    virtual void notifyBeforePageFlush(CachePage &page);
    
    /**
     * Called by CacheImpl when a page flush completes successfully.  This
     * notification method is called with the page mutex held, so the
     * implementation must take care to avoid deadlock.
     *
     * @param page the page that was flushed
     */
    virtual void notifyAfterPageFlush(CachePage &page);
};

FENNEL_END_NAMESPACE

#endif

// End MappedPageListener.h
