//package tools.demo.clickhouse;
//
//import com.virtusai.clickhouseclient.ClickHouseClient;
//import com.virtusai.clickhouseclient.models.http.ClickHouseResponse;
//
//import java.util.Iterator;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ExecutionException;
//
//public class connect111 {
//    public static void main(String[] args) throws InterruptedException {
//
//        ClickHouseClient client = new ClickHouseClient("http://192.168.2.93:8123", "default", "");
//       // ClickHouseClient client = new ClickHouseClient("http://192.168.2.93:8123", "clickh", "1Mlgy");//SELECT * from test.student
//        CompletableFuture<ClickHouseResponse<Metricabcd>> abc = client.get(" Select id from test.student limit 10", Metricabcd.class);//.thenAccept(res -> System.out.println("#######" + res));
//        try {
//            System.out.println("#####11#####"+abc.get().rows.toString());
//            System.out.println("#####22#####"+abc.get().data.size());
//            System.out.println("#####33#####"+abc.get().meta.toArray());
//
//
//            Iterator resIter = abc.get().data.iterator();
//            while (resIter.hasNext()){
//                Object resClass = resIter.next();
//                System.out.println("#####44#####"+resClass.toString());
//               // System.out.println("#####44#####"+resIter.next());
//            }
//
//            Iterator metaIter = abc.get().meta.iterator();
//            while (metaIter.hasNext()){
//                System.out.println("#####555###"+metaIter.next());
//
//            }
//
//
//            System.out.println("#####66#####"+abc.get().statistics.bytes_read);
//
//            System.out.println("#####77#####"+abc.get());
//
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
//        Thread.sleep(2000);
//
//        client.close();
//    }
//
//
//
////    clickhouse 节点192.168.2.93 user:clickh pw：1Mlgy
//
//
//
//
//    // Retrieve data
//
//
//}
//
//
//
//
//class Metricabcd {
//
//
//    private int resnumber;
//
//    public Metricabcd(int resnumber) {
//        this.resnumber = resnumber;
//    }
//
//    public int getResnumber() {
//        return resnumber;
//    }
//
//    public void setResnumber(int resnumber) {
//        this.resnumber = resnumber;
//    }
//
//    @Override
//    public String toString() {
//        return "Metricabcd{" +
//                "resnumber=" + resnumber +
//                '}';
//    }
//}
//
//
//
//
//
//
//class Metricabc {
//    private int id;
//    private String name;
//    private String password;
//    private int age;
//
//    public Metricabc() {
//    }
//
//    public Metricabc(int id, String name, String password, int age) {
//        this.id = id;
//        this.name = name;
//        this.password = password;
//        this.age = age;
//    }
//
//    public int getId() {
//        return id;
//    }
//
//    public void setId(int id) {
//        this.id = id;
//    }
//
//    public String getName() {
//        return name;
//    }
//
//    public void setName(String name) {
//        this.name = name;
//    }
//
//    public String getPassword() {
//        return password;
//    }
//
//    public void setPassword(String password) {
//        this.password = password;
//    }
//
//    public int getAge() {
//        return age;
//    }
//
//    public void setAge(int age) {
//        this.age = age;
//    }
//
//    @Override
//    public String toString() {
//        return "Metric{" +
//                "id=" + id +
//                ", name='" + name + '\'' +
//                ", password='" + password + '\'' +
//                ", age=" + age +
//                '}';
//    }
//}
