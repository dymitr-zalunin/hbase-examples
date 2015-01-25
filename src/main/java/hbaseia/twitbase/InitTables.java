package hbaseia.twitbase;

import hbaseia.twitbase.hbase.TwitsDAO;
import hbaseia.twitbase.hbase.UsersDAO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.net.URI;

public class InitTables {

    public static void main(String[] args) throws Exception {


        Configuration configuration = HBaseConfiguration.create();

        HBaseAdmin admin = new HBaseAdmin(configuration);

        if (args.length != 0 && "-f".equalsIgnoreCase(args[0])) {
            System.out.println("!!! dropping tables in...");
            for (int i = 5; i > 0; i--) {
                System.out.println(i);
                Thread.sleep(1000);
            }

            if (admin.tableExists(UsersDAO.TABLE_NAME)) {
                System.out.printf("Deleting %s\n", Bytes.toString(UsersDAO.TABLE_NAME));
                if (admin.isTableEnabled(UsersDAO.TABLE_NAME)) {
                    admin.disableTable(UsersDAO.TABLE_NAME);
                }
                admin.deleteTable(UsersDAO.TABLE_NAME);
            }
            if (admin.tableExists(TwitsDAO.TABLE_NAME)) {
                System.out.printf("Deleting %s\n", Bytes.toString(TwitsDAO.TABLE_NAME));
                if (admin.isTableEnabled(TwitsDAO.TABLE_NAME)) {
                    admin.disableTable(TwitsDAO.TABLE_NAME);
                }
                admin.deleteTable(TwitsDAO.TABLE_NAME);
            }
        }

        if (admin.tableExists(UsersDAO.TABLE_NAME)) {
            System.out.println("User table already exists.");
        } else {
            System.out.println("Creating User table.");
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(UsersDAO.TABLE_NAME));
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(UsersDAO.INFO_FAM);
            descriptor.addFamily(columnDescriptor);
            admin.createTable(descriptor);
            System.out.println("User table created.");
        }

        if (admin.tableExists(TwitsDAO.TABLE_NAME)) {
            System.out.println("Twits table already exists.");
        } else {
            System.out.println("Creating Twits table.");
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TwitsDAO.TABLE_NAME));
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(TwitsDAO.TWITS_FAM);
            columnDescriptor.setMaxVersions(1);
            descriptor.addFamily(columnDescriptor);
            admin.createTable(descriptor);
            System.out.println("Twits table created.");
        }
    }

}