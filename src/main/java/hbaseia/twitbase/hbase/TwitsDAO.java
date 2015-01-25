package hbaseia.twitbase.hbase;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import sun.security.provider.MD5;
import utils.Md5Utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

public class TwitsDAO {

    public static final byte[] TABLE_NAME = Bytes.toBytes("twits");
    public static final byte[] TWITS_FAM = Bytes.toBytes("twits");

    public static final byte[] USER_COL = Bytes.toBytes("user");
    public static final byte[] TWIT_COL = Bytes.toBytes("twit");
    private static final int longLength = 8; // bytes

    private static final Logger log = Logger.getLogger(UsersDAO.class);


    private HConnection connection;

    public TwitsDAO(HConnection connection) {
        this.connection = connection;
    }


    private byte[] mkRowKey(String user, DateTime dt) {
        byte[] userHash = Md5Utils.md5sum(user);
        //multiply by -1 for descending order
        byte[] timestamp = Bytes.toBytes(-1 * dt.getMillis());
        byte[] rowKey = new byte[Md5Utils.MD5_LENGTH + longLength];
        int offset = Bytes.putBytes(rowKey, 0, userHash, 0, userHash.length);
        Bytes.putBytes(rowKey, offset, timestamp, 0, timestamp.length);
        return rowKey;
    }

    private byte[] mkRowKey(Twit twit) {
        return mkRowKey(twit.user, twit.dt);
    }

    private Put mkPut(Twit twit) {
        Put put = new Put(mkRowKey(twit));
        put.add(TWITS_FAM, USER_COL, Bytes.toBytes(twit.user));
        put.add(TWITS_FAM, TWIT_COL, Bytes.toBytes(twit.text));
        return put;
    }

    private Get mkGet(String user, DateTime dt) {
        Get get = new Get(mkRowKey(user, dt));
        get.addColumn(TWITS_FAM, USER_COL);
        get.addColumn(TWITS_FAM, TWIT_COL);
        return get;
    }

    public void postTwit(String user, DateTime dt, String text) throws IOException {
        HTableInterface twits = connection.getTable(TABLE_NAME);

        Put put = mkPut(new Twit(user, dt, text));
        twits.put(put);

        twits.close();
    }

    public hbaseia.twitbase.model.Twit getTwit(String user, DateTime dt) throws IOException {
        HTableInterface twits = connection.getTable(TABLE_NAME);

        Get get = mkGet(user, dt);
        Result result = twits.get(get);
        if (result.isEmpty()) {
            return null;
        }

        Twit twit = new Twit(result);

        twits.close();
        return twit;
    }

    private static class Twit extends hbaseia.twitbase.model.Twit {

        private Twit(Result r) {
            this(
                    CellUtil.cloneValue(r.getColumnLatestCell(TWITS_FAM, USER_COL)),
                    Arrays.copyOfRange(r.getRow(), Md5Utils.MD5_LENGTH, Md5Utils.MD5_LENGTH + longLength),
                    CellUtil.cloneValue(r.getColumnLatestCell(TWITS_FAM, TWIT_COL)));
        }

        private Twit(byte[] user, byte[] dt, byte[] text) {
            this(
                    Bytes.toString(user),
                    new DateTime(-1 * Bytes.toLong(dt)),
                    Bytes.toString(text));
        }

        private Twit(String user, DateTime dt, String text) {
            this.user = user;
            this.dt = dt;
            this.text = text;
        }
    }
}
