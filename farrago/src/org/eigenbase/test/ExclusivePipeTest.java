/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package org.eigenbase.test;

import junit.framework.TestCase;
import org.eigenbase.runtime.ExclusivePipe;

import java.nio.ByteBuffer;

/**
 * Testcase for {@link org.eigenbase.runtime.ExclusivePipe}.
 */
public class ExclusivePipeTest extends TestCase
{
    private static final int BUF_BYTES = 10;
    private static final int timeoutMillis = Integer.MAX_VALUE;

    public void test()
    {
        ByteBuffer buf = ByteBuffer.allocateDirect(BUF_BYTES);
        ExclusivePipe pipe = new ExclusivePipe(buf);
        Producer producer = new Producer(pipe);
        Consumer consumer = new Consumer(pipe);
        producer.start();
        consumer.start();
        try {
            producer.join(timeoutMillis);
        } catch (InterruptedException e) {
            fail("producer interrupted");
        }
        try {
            consumer.join(timeoutMillis);
        } catch (InterruptedException e) {
            fail("consumer interrupted");
        }
        if (producer.thrown != null) {
            fail("producer had error: " + producer.thrown);
        }
        assertTrue("producer blocked", producer.succeeded);
        if (consumer.thrown != null) {
            fail("producer had error: " + consumer.thrown);
        }
        assertTrue("consumer blocked", consumer.succeeded);
    }

    private static final String[] words = {
        "the", "quick", "brown", "fox", "jumped", "over", "the", "lazy", "dog",
    };

    /**
     * Producer thread writes a list of words into a pipe.
     */
    private static class Producer extends Thread {
        private final ExclusivePipe pipe;
        private boolean succeeded;
        private Throwable thrown;

        Producer(ExclusivePipe pipe) {
            this.pipe = pipe;
        }

        public void run() {
            try {
                ByteBuffer buf = pipe.getBuffer();
                for (int i = 0; i < words.length; i++) {
                    String word = words[i];
                    byte[] bytes = word.getBytes();
                    pipe.beginWriting();
                    // Store the string as a 1-byte length followed by n bytes.
                    // Can't handle strings longer than 255 but hey, this is
                    // only a test!
                    buf.put((byte) bytes.length);
                    buf.put(bytes, 0, bytes.length);
                    pipe.endWriting();
                }
                succeeded = true;
            } catch (Exception e) {
                thrown = e;
                e.printStackTrace();
            }
        }
    }

    /**
     * Consumer thread reads words from a pipe, comparing with the list of
     * expected words, until it has read all of the words it expects to see.
     */
    private static class Consumer extends Thread {
        private final ExclusivePipe pipe;
        private final byte[] bytes = new byte[BUF_BYTES];
        private boolean succeeded;
        private Throwable thrown;

        Consumer(ExclusivePipe pipe) {
            this.pipe = pipe;
        }

        public void run() {
            try {
                ByteBuffer buf = pipe.getBuffer();
                for (int i = 0; i < words.length; i++) {
                    String word = words[i];
                    pipe.beginReading();
                    int length = buf.get();
                    buf.get(bytes, 0, length);
                    String actualWord = new String(bytes, 0, length);
                    assertEquals(word, actualWord);
                    pipe.endReading();
                }
                succeeded = true;
            } catch (Exception e) {
                thrown = e;
                e.printStackTrace();
            }
        }
    }
}
