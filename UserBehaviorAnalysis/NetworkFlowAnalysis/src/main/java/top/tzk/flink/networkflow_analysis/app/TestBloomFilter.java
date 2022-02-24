package top.tzk.flink.networkflow_analysis.app;

public class TestBloomFilter {
    public static void main(String[] args) {
        String a = "abc";
        String b = "abc";
        String c = "abce";
        String d = "abc";
        String e = "abcd";

        System.out.println(1<<28);

        UniqueVisitorApp.MyBloomFilter filter = new UniqueVisitorApp.MyBloomFilter(1 << 28, 3421);

        System.out.println(filter.exists(a));
        System.out.println(filter.exists(b));
        System.out.println(filter.exists(c));
        System.out.println(filter.exists(d));
        System.out.println(filter.exists(e));
    }
}
