package hbaseia.twitbase;

import hbaseia.twitbase.hbase.TwitsDAO;
import hbaseia.twitbase.hbase.UsersDAO;
import hbaseia.twitbase.model.Twit;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;

public class TwitsTool {


    private static final Logger log = Logger.getLogger(TwitsTool.class);

    public static final String usage =
            "twitstool action ...\n" +
                    "  help - print this message and exit.\n" +
                    "  post user text - post a new twit on user's behalf.\n" +
                    "  list user - list all twits for the specified user.\n";

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || "help".equals(args[0])) {
            System.out.println(usage);
            System.exit(0);
        }

        Configuration configuration = HBaseConfiguration.create();
        HConnection connection = HConnectionManager.createConnection(configuration);
        TwitsDAO twitsDao = new TwitsDAO(connection);
        UsersDAO usersDAO = new UsersDAO(connection);

        if ("post".equals(args[0])) {
            DateTime now = new DateTime();
            log.debug(String.format("Posting twit at ...", now));
            twitsDao.postTwit(args[1], now, args[2]);
            Twit t = twitsDao.getTwit(args[1], now);
            usersDAO.incTweetCount(args[1]);
            System.out.println("Successfully posted " + t);
        }

        if ("list".equals(args[0])) {
            List<Twit> twits = twitsDao.list(args[1]);
            log.info(String.format("Found %s twits.", twits.size()));
            for (Twit t : twits) {
                System.out.println(t);
            }
        }

        connection.close();
    }

}
