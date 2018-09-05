import java.util.Arrays;

public class JoinSimulation {
    public static void main(String[] args) {
        generateData();
    }

    private static void generateData() {
        DataGenerator dg = new DataGenerator(DataGenerator.KeyType.NUMERIC, 10000000,
                Arrays.asList(new DataGenerator.Attribute(20), new DataGenerator.Attribute(100),
                        new DataGenerator.Attribute(80)),2);

        dg.write("1000000.csv");
    }
}
