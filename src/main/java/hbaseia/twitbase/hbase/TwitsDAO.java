package hbaseia.twitbase.hbase;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import utils.Md5Utils;

import java.util.Arrays;

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
