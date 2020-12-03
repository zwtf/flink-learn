
import org.junit.Test;


import java.text.SimpleDateFormat;

public class TestClass {

    @Test
    public void TestCase() {


        Long startTimestamp = 1511658000L;
        Long endTimestamp = 1511690400L;

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
/*        System.out.println(new com.sun.jmx.snmp.Timestamp(startTimestamp * 1000 - 1));
        System.out.println(new com.sun.jmx.snmp.Timestamp(endTimestamp * 1000 - 1));*/

        System.out.println(new java.sql.Timestamp(startTimestamp * 1000));
        System.out.println(new  java.sql.Timestamp(endTimestamp * 1000));
        System.out.println(new  java.sql.Timestamp(0));
        System.out.println(new  java.sql.Timestamp(1512000000));
        System.out.println(new  java.sql.Timestamp(1511700000));
        System.out.println(new  java.sql.Timestamp(1512000000));
        System.out.println(new  java.sql.Timestamp(1514400000));
        System.out.println(new  java.sql.Timestamp(1514400000 * 1000));




    }

}
