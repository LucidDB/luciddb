package com.lucidera.luciddb.applib.impexp;

import java.io.EOFException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Purpose: Allow serialized rows to be streamed via HTTP from remote Java
 * applications (PDI / Talend).<br>
 * Please refer to http://pub.eigenbase.org/wiki/LucidDbAppLib_REMOTE_ROWS<br>
 * 
 * @author Ray Zhang
 * @since Dec-16-2009
 */
public class RemoteRowsUDX
{

    public static void execute(
        ResultSet inputSet,
        int port,
        boolean is_compressed,
        PreparedStatement resultInserter)
        throws Exception
    {

        ServerSocket ss = new ServerSocket(port);
        Socket socket = null;

        while (true) {

            try {

                socket = ss.accept();
                InputStream sIn = socket.getInputStream();
                GZIPInputStream gzIn = null;
                ObjectInputStream objIn = null;

                if (is_compressed) {

                    gzIn = new GZIPInputStream(sIn);
                    objIn = new ObjectInputStream(gzIn);

                } else {

                    objIn = new ObjectInputStream(sIn);

                }

                boolean is_header = true;
                int row_counter = 0;

                while (true) {

                    try {

                        List entity = (ArrayList) objIn.readObject();

                        // disable header format check.
                        if (is_header) {

                            // check if header info is matched.
                            // List header_from_cursor =
                            // getHeaderInfoFromCursor(inputSet);
                            // List header_from_file = (ArrayList)
                            // entity.get(1);
                            //
                            // if (verifyHeaderInfo(
                            // header_from_cursor,
                            // header_from_file))
                            // {
                            //
                            // is_header = false;
                            //
                            // } else {
                            //
                            // throw new Exception(
                            // "Header Info was unmatched! Please check");
                            // }

                            is_header = false;

                        } else {

                            int col_count = entity.size();
                            for (int i = 0; i < col_count; i++) {

                                resultInserter.setObject((i + 1), entity.get(i));

                            }
                            resultInserter.executeUpdate();
                            row_counter++;
                        }

                    } catch (EOFException ex) {

                        break;

                    } catch (Exception e) {

                        // release resource.
                        objIn.close();

                        if (is_compressed) {

                            gzIn.close();
                        }
                        sIn.close();

                        throw new Exception("Error: " + e.getMessage() + "\n"
                            + row_counter + " rows are inserted successfully.");
                    }

                }

                // release all resources.

                objIn.close();

                if (is_compressed) {

                    gzIn.close();
                }
                sIn.close();

                if (is_header == false) {

                    socket.close();
                    break;

                }

            } catch (Exception ex) {

                if (socket != null) {

                    socket.close();
                }

                ss.close();
                throw ex;

            }

        }

        ss.close();
    }

    protected static boolean verifyHeaderInfo(
        List header_from_cursor,
        List header_from_file)
    {

        boolean is_matched = false;

        // 1. check column raw count
        if (header_from_cursor.size() == header_from_file.size()) {

            // 2. check the length of every field.
            int col_raw_count = header_from_cursor.size();

            for (int i = 0; i < col_raw_count; i++) {

                int length_of_field_from_cursor = (Integer) header_from_cursor.get(i);
                int length_of_field_from_file = (Integer) header_from_file.get(i);
                if (length_of_field_from_cursor == length_of_field_from_file) {

                    is_matched = true;

                } else {

                    is_matched = false;
                    break;
                }
            }

        }

        return is_matched;
    }

    protected static List getHeaderInfoFromCursor(ResultSet rs_in)
        throws SQLException
    {

        int columnCount = rs_in.getMetaData().getColumnCount();
        List ret = new ArrayList(columnCount);
        for (int i = 0; i < columnCount; i++) {

            ret.add(rs_in.getMetaData().getColumnDisplaySize(i + 1));
        }

        return ret;

    }

}
