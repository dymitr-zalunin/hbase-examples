package hbaseia.twitbase.hbase;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class TwitsDAO {

    public static final byte[] TABLE_NAME = Bytes.toBytes("twits");
    public static final byte[] TWITS_FAM = Bytes.toBytes("twits");

    public static final byte[] USER_COL = Bytes.toBytes("user");
    public static final byte[] TWIT_COL = Bytes.toBytes("twit");

    private static final Logger log = Logger.getLogger(UsersDAO.class);

    private HConnection connection;

    public TwitsDAO(HConnection connection) {
        this.connection = connection;
    }
    
    
}
