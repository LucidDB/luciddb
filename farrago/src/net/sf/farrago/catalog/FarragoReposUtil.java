/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package net.sf.farrago.catalog;

import java.io.*;

import java.net.*;

import java.nio.charset.*;

import java.util.*;
import java.util.zip.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;
import javax.jmi.xmi.*;

import net.sf.farrago.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.enki.mdr.*;
import org.eigenbase.jmi.*;
import org.eigenbase.util.*;

import org.netbeans.api.mdr.*;
import org.netbeans.api.xmi.*;


/**
 * Static utilities for manipulating the Farrago repository.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoReposUtil
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String FARRAGO_CATALOG_EXTENT = "FarragoCatalog";
    public static final String FARRAGO_METAMODEL_EXTENT = "FarragoMetamodel";
    public static final String FARRAGO_PACKAGE_NAME = "Farrago";

    //~ Methods ----------------------------------------------------------------

    /**
     * Exports a submodel, generating qualified references by name to objects
     * outside of the submodel.
     *
     * @param mdrRepos MDR repository containing submodel to export
     * @param outputFile file into which XMI should be written
     * @param subPackageName name of package containing submodel to be exported
     */
    public static void exportSubModel(
        MDRepository mdrRepos,
        File outputFile,
        String subPackageName)
        throws Exception
    {
        XMIWriter xmiWriter = XMIWriterFactory.getDefault().createXMIWriter();
        ExportRefProvider refProvider =
            new ExportRefProvider(
                subPackageName);
        xmiWriter.getConfiguration().setReferenceProvider(refProvider);
        FileOutputStream outStream = new FileOutputStream(outputFile);
        try {
            xmiWriter.write(
                outStream,
                "SUBMODEL",
                mdrRepos.getExtent(FARRAGO_METAMODEL_EXTENT),
                "1.2");
            if (!refProvider.subPackageFound) {
                throw new NoSuchElementException(subPackageName);
            }
        } finally {
            outStream.close();
        }
    }

    public static void importSubModel(
        MDRepository mdrRepos,
        URL inputUrl)
        throws Exception
    {
        if (((EnkiMDRepository) mdrRepos).isExtentBuiltIn(
                FARRAGO_METAMODEL_EXTENT))
        {
            return;
        }

        XMIReader xmiReader = XMIReaderFactory.getDefault().createXMIReader();
        ImportRefResolver refResolver =
            new ImportRefResolver(
                (Namespace) mdrRepos.getExtent(FARRAGO_CATALOG_EXTENT)
                                    .refMetaObject());
        xmiReader.getConfiguration().setReferenceResolver(refResolver);
        boolean rollback = false;
        try {
            mdrRepos.beginTrans(true);
            rollback = true;

            InvalidXmlCharFilterInputStream filter =
                new InvalidXmlCharFilterInputStream(inputUrl.openStream());

            xmiReader.read(
                filter,
                inputUrl.toString(),
                mdrRepos.getExtent(FARRAGO_METAMODEL_EXTENT));

            if (filter.getNumInvalidCharsFiltered() > 0) {
                FarragoTrace.getReposTracer().warning(
                    "Filtered " + filter.getNumInvalidCharsFiltered()
                    + " invalid characters from XMI at '"
                    + inputUrl.toString() + "'");
            }

            rollback = false;
            mdrRepos.endTrans();
        } finally {
            if (rollback) {
                mdrRepos.endTrans(true);
            }
        }
    }

    public static void dumpRepository()
        throws Exception
    {
        dumpRepository(new FarragoModelLoader());
    }

    public static void dumpRepository(
        FarragoModelLoader modelLoader)
        throws Exception
    {
        dumpRepository(modelLoader, false);
    }

    public static void dumpRepository(
        FarragoModelLoader modelLoader,
        boolean metamodelDumpOnly)
        throws Exception
    {
        FarragoProperties farragoProps = modelLoader.getFarragoProperties();
        File catalogDir = farragoProps.getCatalogDir();
        File metamodelDump = new File(catalogDir, "FarragoMetamodelDump.xmi");
        File catalogDump = new File(catalogDir, "FarragoCatalogDump.xmi");

        boolean success = false;
        try {
            FarragoPackage farragoPackage =
                modelLoader.loadModel(FARRAGO_CATALOG_EXTENT, false);

            modelLoader.getMdrRepos().beginTrans(false);
            try {
                exportExtent(
                    modelLoader.getMdrRepos(),
                    metamodelDump,
                    FARRAGO_METAMODEL_EXTENT);
                if (!metamodelDumpOnly) {
                    exportExtent(
                        modelLoader.getMdrRepos(),
                        catalogDump,
                        FARRAGO_CATALOG_EXTENT);
                }
            } finally {
                modelLoader.getMdrRepos().endTrans();
            }

            deleteStorage(modelLoader, farragoPackage);
            success = true;
        } finally {
            // Close session started in modelLoader.loadModel
            modelLoader.close();
            if (!success) {
                metamodelDump.delete();
                catalogDump.delete();
            }
        }
    }

    /**
     * @deprecated pass FarragoModelLoader parameter
     */
    public static boolean isReloadNeeded()
    {
        return isReloadNeeded(new FarragoModelLoader());
    }

    public static boolean isReloadNeeded(FarragoModelLoader modelLoader)
    {
        File catalogDir = modelLoader.getFarragoProperties().getCatalogDir();
        return new File(catalogDir, "FarragoMetamodelDump.xmi").exists();
    }

    /**
     * @deprecated pass FarragoModelLoader parameter
     */
    public static void reloadRepository()
        throws Exception
    {
        reloadRepository(new FarragoModelLoader());
    }

    public static void reloadRepository(
        FarragoModelLoader modelLoader)
        throws Exception
    {
        File catalogDir = modelLoader.getFarragoProperties().getCatalogDir();
        File metamodelDump = new File(catalogDir, "FarragoMetamodelDump.xmi");
        String catalogDumpName = "FarragoCatalogDump.xmi";
        File catalogDump = new File(catalogDir, catalogDumpName);

        // If FarragoCatalogDump.xmi doesn't exist, assume the dump is
        // compressed using gzip.
        boolean isCompressed = false;
        if (!catalogDump.exists()) {
            catalogDumpName += ".gz";
            catalogDump = new File(catalogDir, catalogDumpName);
            isCompressed = true;
        }

        try {
            modelLoader.initStorage(false);

            // import metamodel
            importExtent(
                modelLoader.getMdrRepos(),
                metamodelDump,
                FARRAGO_METAMODEL_EXTENT,
                null,
                null,
                false);

            // import catalog
            importExtent(
                modelLoader.getMdrRepos(),
                catalogDump,
                FARRAGO_CATALOG_EXTENT,
                FARRAGO_METAMODEL_EXTENT,
                FARRAGO_PACKAGE_NAME,
                isCompressed);

            metamodelDump.delete();
            catalogDump.delete();
        } finally {
            modelLoader.close();
        }
    }

    public static void exportExtent(
        MDRepository mdrRepos,
        File file,
        String extentName)
        throws Exception
    {
        exportExtent(mdrRepos, file, extentName, false);
    }

    public static void exportExtent(
        MDRepository mdrRepos,
        File file,
        String extentName,
        boolean isCompressed)
        throws Exception
    {
        RefPackage refPackage = mdrRepos.getExtent(extentName);
        XmiWriter xmiWriter = XMIWriterFactory.getDefault().createXMIWriter();
        OutputStream outStream = new FileOutputStream(file);
        if (isCompressed) {
            outStream = new GZIPOutputStream(outStream);
        }
        try {
            xmiWriter.write(outStream, refPackage, "1.2");
        } finally {
            outStream.close();
        }
    }

    private static void deleteStorage(
        FarragoModelLoader modelLoader,
        FarragoPackage farragoPackage)
        throws Exception
    {
        EnkiMDRepository repos = (EnkiMDRepository) modelLoader.getMdrRepos();

        repos.dropExtentStorage(farragoPackage);
    }

    private static void importExtent(
        MDRepository mdrRepos,
        File file,
        String extentName,
        String metaPackageExtentName,
        String metaPackageName,
        boolean isCompressed)
        throws Exception
    {
        mdrRepos.beginTrans(true);
        boolean rollback = true;
        try {
            RefPackage extent;
            if (metaPackageExtentName != null) {
                ModelPackage modelPackage =
                    (ModelPackage) mdrRepos.getExtent(metaPackageExtentName);
                MofPackage metaPackage = null;
                for (Object o : modelPackage.getMofPackage().refAllOfClass()) {
                    MofPackage result = (MofPackage) o;
                    if (result.getName().equals(metaPackageName)) {
                        metaPackage = result;
                        break;
                    }
                }
                extent = mdrRepos.createExtent(extentName, metaPackage);
            } else {
                if (((EnkiMDRepository) mdrRepos).isExtentBuiltIn(extentName)) {
                    // Go ahead and rollback; we haven't changed anything.
                    return;
                }

                extent = mdrRepos.createExtent(extentName);
            }

            XmiReader xmiReader =
                XMIReaderFactory.getDefault().createXMIReader();

            InputStream in = new FileInputStream(file);
            if (isCompressed) {
                in = new GZIPInputStream(in);
            }
            InvalidXmlCharFilterInputStream filter =
                new InvalidXmlCharFilterInputStream(in);

            xmiReader.read(
                filter,
                file.toURL().toString(),
                extent);

            if (filter.getNumInvalidCharsFiltered() > 0) {
                FarragoTrace.getReposTracer().warning(
                    "Filtered " + filter.getNumInvalidCharsFiltered()
                    + " invalid characters from XMI file '"
                    + file.getAbsolutePath() + "'");
            }

            rollback = false;
        } finally {
            mdrRepos.endTrans(rollback);
        }
    }

    private static void mainExportSubModel(String [] args)
        throws Exception
    {
        assert (args.length == 3);
        File file = new File(args[1]);
        String subPackageName = args[2];
        FarragoModelLoader modelLoader = new FarragoModelLoader();
        try {
            modelLoader.loadModel(FARRAGO_CATALOG_EXTENT, false);
            exportSubModel(
                modelLoader.getMdrRepos(),
                file,
                subPackageName);
        } finally {
            modelLoader.close();
        }
    }

    private static void mainImportSubModel(String [] args)
        throws Exception
    {
        assert (args.length == 2);
        File file = new File(args[1]);
        FarragoModelLoader modelLoader = new FarragoModelLoader();
        try {
            modelLoader.loadModel(FARRAGO_CATALOG_EXTENT, false);
            importSubModel(
                modelLoader.getMdrRepos(),
                file.toURL());
        } finally {
            modelLoader.close();
        }
    }

    public static void main(String [] args)
        throws Exception
    {
        // TODO:  proper arg checking
        assert (args.length > 0);
        if (args[0].equals("exportSubModel")) {
            mainExportSubModel(args);
        } else if (args[0].equals("importSubModel")) {
            mainImportSubModel(args);
        } else {
            throw new IllegalArgumentException(args[0]);
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class ExportRefProvider
        implements XMIReferenceProvider
    {
        private final String subPackageName;

        boolean subPackageFound;

        ExportRefProvider(
            String subPackageName)
        {
            this.subPackageName = subPackageName;
        }

        // implement XMIReferenceProvider
        public XMIReferenceProvider.XMIReference getReference(RefObject obj)
        {
            RefObject parent = obj;
            if (obj instanceof Tag) {
                Collection c = ((Tag) obj).getElements();
                if (c.size() == 1) {
                    parent = (RefObject) c.iterator().next();
                }
            }
            List<String> nameList = new ArrayList<String>();
            do {
                String name = (String) parent.refGetValue("name");
                nameList.add(name);
                if (subPackageName.equals(name)) {
                    subPackageFound = true;
                    return new XMIReferenceProvider.XMIReference(
                        "SUBMODEL",
                        Long.toString(JmiObjUtil.getObjectId(obj)));
                }
                parent = (RefObject) parent.refImmediateComposite();
            } while (parent != null);
            Collections.reverse(nameList);
            int k = 0;
            StringBuilder sb = new StringBuilder();
            for (String name : nameList) {
                if (k++ > 0) {
                    sb.append('/');
                }
                sb.append(name);
            }
            return new XMIReferenceProvider.XMIReference(
                "REPOS",
                sb.toString());
        }
    }

    private static class ImportRefResolver
        implements XMIReferenceResolver
    {
        private final Namespace root;

        ImportRefResolver(Namespace root)
        {
            this.root = root;
        }

        // implement XMIReferenceResolver
        public void register(
            String systemId,
            String xmiId,
            RefObject object)
        {
            // don't care
        }

        // implement XMIReferenceResolver
        public void resolve(
            XMIReferenceResolver.Client client,
            RefPackage extent,
            String systemId,
            XMIInputConfig configuration,
            Collection hrefs)
        {
            Iterator iter = hrefs.iterator();
            while (iter.hasNext()) {
                String href = iter.next().toString();
                int nameStart = href.indexOf('#') + 1;
                assert (nameStart != 0);
                String [] names = href.substring(nameStart).split("/");
                assert (names[0].equals(root.getName()));
                Namespace ns = root;
                try {
                    for (int i = 1; i < (names.length - 1); ++i) {
                        ns = (Namespace) ns.lookupElement(names[i]);
                    }
                    ModelElement element;
                    if (names.length == 1) {
                        element = (ModelElement) ns;
                    } else {
                        element = ns.lookupElement(names[names.length - 1]);
                    }
                    client.resolvedReference(href, element);
                } catch (NameNotFoundException ex) {
                    throw Util.newInternal(ex);
                }
            }
        }
    }

    public static class InvalidXmlCharFilterInputStream
        extends InputStream
    {
        // Various common byte order marks (BOMs) to detect -- normally
        // done by the XML parser, but we need to be able to decode characters
        // from the stream before they get that far.
        private static final byte [] UTF16_BE_BOM =
            toBytes(
                new int[] {
                    0xFE, 0xFF, // <byte order mark>
                    0x00, 0x3C, // <
                    0x00, 0x3F, // ?
                    0x00, 0x78, // x
                    0x00, 0x6D, // m
                    0x00, 0x6C, // l
                    0x00, 0x20, // <space>
                });

        private static final byte [] UTF16_BE_SANS_BOM =
            toBytes(
                new int[] {
                    0x00, 0x3C, // <
                    0x00, 0x3F, // ?
                    0x00, 0x78, // x
                    0x00, 0x6D, // m
                    0x00, 0x6C, // l
                    0x00, 0x20, // <space>
                });

        private static final byte [] UTF16_LE_BOM =
            toBytes(
                new int[] {
                    0xFF, 0xFE, // <byte order mark>
                    0x3C, 0x00, // <
                    0x3F, 0x00, // ?
                    0x78, 0x00, // x
                    0x6D, 0x00, // m
                    0x6C, 0x00, // l
                    0x20, 0x00, // <space>
                });

        private static final byte [] UTF16_LE_SANS_BOM =
            toBytes(
                new int[] {
                    0x3C, 0x00, // <
                    0x3F, 0x00, // ?
                    0x78, 0x00, // x
                    0x6D, 0x00, // m
                    0x6C, 0x00, // l
                    0x20, 0x00, // <space>
                });

        private static final byte [] UTF8_BOM =
            toBytes(
                new int[] {
                    0xEF, 0xBB, 0xBF, // byte order mark
                    0x3C, // <
                    0x3F, // ?
                    0x78, // x
                    0x6D, // m
                    0x6C, // l
                    0x20, // <space>
                });

        private static final byte [] OTHER_ASCII_LIKE =
            toBytes(
                new int[] {
                    0x3C, // <
                    0x3F, // ?
                    0x78, // x
                    0x6D, // m
                    0x6C, // l
                    0x20, // <space>
                });

        private static final Map<byte[], String> ALL_DECLS;

        static {
            HashMap<byte[], String> decls = new HashMap<byte[], String>();
            decls.put(UTF16_BE_BOM, "UTF-16BE");
            decls.put(UTF16_BE_SANS_BOM, "UTF-16BE");
            decls.put(UTF16_LE_BOM, "UTF-16LE");
            decls.put(UTF16_LE_SANS_BOM, "UTF-16LE");
            decls.put(UTF8_BOM, "UTF-8");
            decls.put(OTHER_ASCII_LIKE, "");
            ALL_DECLS = Collections.unmodifiableMap(decls);
        }

        // Choose a reasonable upper limit for the maximum length of an XML
        // declaration, including encoding name.
        private static final int MAX_DECL_SIZE = 256;

        private final InputStream in;
        private int numInvalidCharsFiltered;
        private ByteOutputStream outputBuffer;
        private Writer outputBufferWriter;
        private Reader reader;
        private char [] inputBuffer;

        public InvalidXmlCharFilterInputStream(InputStream in)
            throws IOException
        {
            if (!in.markSupported()) {
                in = new BufferedInputStream(in);
            }
            this.in = in;
            this.numInvalidCharsFiltered = 0;
            this.outputBuffer = new ByteOutputStream(4096);

            Charset charset = guessCharset(in, outputBuffer);

            // Assume maximum encoding overhead is 2 bytes per char.
            in.mark(2 * MAX_DECL_SIZE);
            this.reader =
                new BufferedReader(new InputStreamReader(in, charset));

            Charset encodedCharset = getCharsetFromXmlDecl(reader);
            if ((encodedCharset != null) && !charset.equals(encodedCharset)) {
                boolean sameButNoEndianness = false;
                String encodedName = encodedCharset.name();
                String name = charset.name();
                if (name.startsWith(encodedName)) {
                    String suffix = name.substring(encodedName.length());

                    sameButNoEndianness =
                        suffix.equals("LE") || suffix.equals("BE");
                }

                // Ignore the case where we properly detect, say UTF-16 LE,
                // but the XML decl doesn't specify byte order.  The Java
                // UTF-16 Charset implementation assumes network byte order
                // (UTF-16 BE) which would cause problems.
                if (!sameButNoEndianness) {
                    charset = encodedCharset;
                    in.reset();
                    this.reader = new InputStreamReader(in, charset);
                }
            }

            this.outputBufferWriter =
                new OutputStreamWriter(outputBuffer, charset);

            this.inputBuffer =
                new char[(int) Math.round(
                    charset.newDecoder().averageCharsPerByte() * 4096.0)];
        }

        /**
         * Converts an int array to byte array. Assumes all int values in the
         * array contain only 8 bits of data.
         *
         * @param data int array
         *
         * @return byte array
         */
        private static byte [] toBytes(int [] data)
        {
            byte [] bytes = new byte[data.length];
            for (int i = 0; i < data.length; i++) {
                bytes[i] = (byte) (data[i] & 0xFF);
            }
            return bytes;
        }

        /**
         * Compares two bytes arrays. If the data array does not begin with
         * exactly the bytes specified in the expected array, returns false. The
         * data array may be longer than the expected array, but not shorter.
         *
         * @param data data to test
         * @param expected expected value
         *
         * @return true if data and expected match (see above)
         */
        private static boolean matches(byte [] data, byte [] expected)
        {
            final int len = expected.length;
            if (len > data.length) {
                return false;
            }

            for (int i = 0; i < len; i++) {
                if (data[i] != expected[i]) {
                    return false;
                }
            }

            return true;
        }

        /**
         * Guesses the character set used by the input stream, storing
         * characters in the buffer stream. The first block of data in the input
         * stream is compared against the XML declarations in {@link #ALL_DECLS}
         * to find a characters set suitable for reading at least the XML
         * declaration from the input stream.
         *
         * @param in input stream
         * @param bufferStream buffer stream for temporary storage
         *
         * @return best-guess character set for the input stream
         *
         * @throws IOException on I/O error, if the character set cannot be
         * detected or instantiated
         */
        private static Charset guessCharset(
            InputStream in,
            ByteOutputStream bufferStream)
            throws IOException
        {
            in.mark(2 * MAX_DECL_SIZE);

            int size = 0;
            while (size < MAX_DECL_SIZE) {
                int bytesRead =
                    in.read(bufferStream.array(), size, MAX_DECL_SIZE - size);
                bufferStream.size(bufferStream.size() + bytesRead);
                if (bytesRead < 0) {
                    break;
                }
                size += bytesRead;
            }
            bufferStream.reset();
            in.reset();

            String charsetName = null;
            for (Map.Entry<byte [], String> entry : ALL_DECLS.entrySet()) {
                if (matches(bufferStream.array(), entry.getKey())) {
                    charsetName = entry.getValue();
                    break;
                }
            }

            if (charsetName == null) {
                throw new IOException(
                    "Unsupported XML encoding (or not a valid XML file)");
            } else if (charsetName.length() == 0) {
                charsetName = "ISO-8859-1";
            }

            return Charset.forName(charsetName);
        }

        /**
         * Parses the XML declaration at the start of the Reader's input and
         * returns the specified encoding, if any. The given Reader must be
         * configured with a character set encoding capable of reading the XML
         * declaration (which will only contain a limited set of characters).
         *
         * @param reader a Reader configured with a suitable character set
         * encoding
         *
         * @return the character set specified by the XML declaration, or null
         * if not found
         *
         * @throws IOException on I/O error or if the named character set cannot
         * be instantiated
         * @throws IndexOutOfBoundsException if the XML declaration is malformed
         */
        private static Charset getCharsetFromXmlDecl(Reader reader)
            throws IOException
        {
            final int max = 1024;

            reader.mark(max);

            try {
                StringBuilder b = new StringBuilder();

                int n = 0;
                while (n < max) {
                    int ch = reader.read();
                    b.append((char) ch);
                    n++;

                    if ((n > 2) && "?>".equals(b.substring(n - 2, n))) {
                        break;
                    }
                }

                int encoding = b.indexOf("encoding");
                if (encoding < 0) {
                    return null;
                }

                try {
                    encoding += 8;
                    while (Character.isWhitespace(b.charAt(encoding))) {
                        encoding++;
                    }

                    if (b.charAt(encoding) != '=') {
                        return null;
                    }
                    encoding++;

                    while (Character.isWhitespace(b.charAt(encoding))) {
                        encoding++;
                    }

                    char quote = b.charAt(encoding);
                    if ((quote != '\'') && (quote != '"')) {
                        return null;
                    }
                    int encodingEnd = ++encoding;
                    while (b.charAt(encodingEnd) != quote) {
                        encodingEnd++;
                    }

                    String charsetName = b.substring(encoding, encodingEnd);
                    if (charsetName.length() == 0) {
                        return null;
                    }

                    return Charset.forName(charsetName);
                } catch (IndexOutOfBoundsException e) {
                    return null;
                }
            } finally {
                reader.reset();
            }
        }

        @Override public boolean markSupported()
        {
            return false;
        }

        @Override public int available()
            throws IOException
        {
            return outputBuffer.remaining();
        }

        @Override public void close()
            throws IOException
        {
            outputBuffer.reset();
            in.close();
        }

        @Override public int read()
            throws IOException
        {
            while (true) {
                // If there are bytes in the output buffer (e.g. from a
                // multi-byte encoding), return them.
                if (outputBuffer.remaining() > 0) {
                    int result = outputBuffer.get();
                    return result & 0xFF;
                }

                // Reset buffer, it's empty.
                outputBuffer.reset();

                // Read the next character from the underlying Reader
                int ch = reader.read();
                if (ch == -1) {
                    return -1;
                }

                if (check(ch)) {
                    // Valid XML, encode it into the output buffer and restart
                    // the loop.
                    outputBufferWriter.write(ch);
                    outputBufferWriter.flush();
                    continue;
                }

                // Invalid character
                FarragoTrace.getMdrTracer().fine(
                    "Invalid XML character: " + Integer.toHexString(ch));
                numInvalidCharsFiltered++;
            }
        }

        private boolean check(int ch)
        {
            return ((ch >= 0x0020) && (ch <= 0xD7FF))
                || ((ch >= 0xE000) && (ch <= 0xFFFD))
                || (ch == 0x0009)
                || (ch == 0x000A)
                || (ch == 0x000D);
        }

        @Override public int read(byte [] b, int off, int len)
            throws IOException
        {
            while (true) {
                // Enough bytes in the output buffer to satisfy the read
                // request, so copy them into b and return.
                if (outputBuffer.remaining() >= len) {
                    outputBuffer.get(b, off, len);
                    return len;
                } else {
                    // Compact the buffer, it might not be empty.
                    outputBuffer.compact();

                    // Read a block of characters.
                    int charsRead = reader.read(inputBuffer);
                    if (charsRead < 0) {
                        // Reader is at EOS, if there are no bytes remaining in
                        // the output buffer, we're done.  Otherwise, restart
                        // the loop with the reduced length so we return the
                        // remnants in the output buffer.
                        len = outputBuffer.remaining();
                        if (len == 0) {
                            return -1;
                        }
                    } else {
                        for (int i = 0; i < charsRead; i++) {
                            char ch = inputBuffer[i];
                            if (check(ch)) {
                                // Valid character, encode it into the output
                                // buffer.
                                outputBufferWriter.write(((int) ch) & 0xFFFF);
                            } else {
                                // Invalid character
                                FarragoTrace.getMdrTracer().fine(
                                    "Invalid XML character: "
                                    + Integer.toHexString(ch));
                                numInvalidCharsFiltered++;
                            }
                        }

                        // Flush the buffer to make sure all bytes reach the
                        // actual output buffer.
                        outputBufferWriter.flush();
                    }
                }
            }
        }

        @Override public long skip(long n)
            throws IOException
        {
            long skip = 0;
            while (skip < n) {
                if (read() < 0) {
                    break;
                }
                skip++;
            }

            return skip;
        }

        public int getNumInvalidCharsFiltered()
        {
            return numInvalidCharsFiltered;
        }

        /**
         * ByteOutputStream extends ByteArrayOutputStream to provide
         * ByteBuffer-like operations such as compact, array and get.
         */
        private static class ByteOutputStream
            extends ByteArrayOutputStream
        {
            private int pos;

            public ByteOutputStream(int initialBufferSize)
            {
                super(initialBufferSize);

                this.pos = 0;
            }

            public void size(int size)
            {
                super.count = Math.min(size, super.buf.length);
            }

            public byte [] array()
            {
                return super.buf;
            }

            public byte get()
            {
                return super.buf[pos++];
            }

            public void get(byte [] b, int off, int len)
            {
                if (len > remaining()) {
                    throw new IndexOutOfBoundsException();
                }

                System.arraycopy(super.buf, pos, b, off, len);
                pos += len;
            }

            public int remaining()
            {
                return size() - pos;
            }

            @Override public void reset()
            {
                super.reset();
                pos = 0;
            }

            public void compact()
            {
                int rem = remaining();
                if (rem > 0) {
                    System.arraycopy(super.buf, pos, super.buf, 0, rem);
                }
                size(rem);
                pos = 0;
            }
        }
    }
}

// End FarragoReposUtil.java
