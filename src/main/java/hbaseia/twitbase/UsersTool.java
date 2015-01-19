package hbaseia.twitbase;

import hbaseia.twitbase.hbase.UsersDAO;
import hbaseia.twitbase.model.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class UsersTool {

    private static final Logger log = Logger.getLogger(UsersTool.class);

    public static final String usage =
            "usertool action ...\n" +
                    "  add user name email password - add a new user.\n";

    public static void main(String[] args) throws IOException {
        if (args.length == 0 || "help".equals(args[0])) {
            System.out.println(usage);
            System.exit(0);
        }

        Configuration configuration = HBaseConfiguration.create();
        HConnection connection = HConnectionManager.createConnection(configuration);
        UsersDAO dao = new UsersDAO(connection);

        if ("add".equals(args[0])) {
            log.debug("Adding user...");
            dao.addUser(args[1], args[2], args[3], args[4]);
            User user = dao.getUser(args[1]);
            System.out.println("Successfully added user" + user);
        }
        
        connection.close();

    }

}