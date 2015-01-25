package hbaseia.twitbase.hbase;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UsersDAO {

    public static final byte[] TABLE_NAME = Bytes.toBytes("users");
    public static final byte[] INFO_FAM = Bytes.toBytes("info");

    public static final byte[] USER_COL = Bytes.toBytes("user");
    public static final byte[] NAME_COL = Bytes.toBytes("name");
    public static final byte[] EMAIL_COL = Bytes.toBytes("email");
    public static final byte[] PASS_COL = Bytes.toBytes("password");
    public static final byte[] TWEETS_COL = Bytes.toBytes("tweet_count");

    private static final Logger log = Logger.getLogger(UsersDAO.class);

    private HConnection connection;

    public UsersDAO(HConnection connection) {
        this.connection = connection;
    }

    public Get mkGet(String user) {
        log.debug(String.format("Creating Get for %s", user));

        Get get = new Get(Bytes.toBytes(user));
        get.addFamily(INFO_FAM);
        return get;
    }

    public Put mkPut(User user) {
        log.debug(String.format("Creating Put for %s", user));

        Put put = new Put(Bytes.toBytes(user.user));
        put.add(INFO_FAM, USER_COL, Bytes.toBytes(user.user));
        put.add(INFO_FAM, NAME_COL, Bytes.toBytes(user.name));
        put.add(INFO_FAM, EMAIL_COL, Bytes.toBytes(user.email));
        put.add(INFO_FAM, PASS_COL, Bytes.toBytes(user.password));
        return put;
    }

    public Scan mkScan() {
        Scan scan = new Scan();
        scan.addFamily(INFO_FAM);
        return scan;
    }

    public hbaseia.twitbase.model.User getUser(String user) throws IOException {
        HTableInterface users = connection.getTable(TABLE_NAME);
        Get g = mkGet(user);
        Result result = users.get(g);
        if (result.isEmpty()) {
            log.info(String.format("user %s not found", user));
            return null;
        }

        User u = new User(result);
        users.close();
        return u;
    }

    public void addUser(String user, String name, String email, String password) throws IOException {
        HTableInterface users = connection.getTable(TABLE_NAME);
        Put put = mkPut(new User(user, name, email, password));
        users.put(put);
        users.close();
    }

    public List<hbaseia.twitbase.model.User> getUsers() throws IOException {
        HTableInterface users = connection.getTable(TABLE_NAME);
        Scan scan = mkScan();
        ResultScanner results = users.getScanner(scan);
        List<hbaseia.twitbase.model.User> ret = new ArrayList<hbaseia.twitbase.model.User>();
        for (Result result : results) {
            ret.add(new User(result));
        }
        users.close();
        return ret;
    }

    public long incTweetCount(String user) throws IOException {
        HTableInterface users = connection.getTable(TABLE_NAME);

        long ret = users.incrementColumnValue(Bytes.toBytes(user), INFO_FAM, TWEETS_COL, 1L);

        users.close();
        return ret;
    }

    private static class User
            extends hbaseia.twitbase.model.User {
        private User(Result r) {
            this(r.getValue(INFO_FAM, USER_COL),
                    r.getValue(INFO_FAM, NAME_COL),
                    r.getValue(INFO_FAM, EMAIL_COL),
                    r.getValue(INFO_FAM, PASS_COL));
        }

        private User(byte[] user,
                     byte[] name,
                     byte[] email,
                     byte[] password) {
            this(Bytes.toString(user),
                    Bytes.toString(name),
                    Bytes.toString(email),
                    Bytes.toString(password));
        }

        private User(String user,
                     String name,
                     String email,
                     String password) {
            this.user = user;
            this.name = name;
            this.email = email;
            this.password = password;
        }
    }

}