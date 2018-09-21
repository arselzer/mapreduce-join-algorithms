package com.alexselzer.mrjoins;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;

public class DataGenerator {
    public static class Attribute {
        private static final int RANDOM_STRINGS = 100;
        private int length;
        private Random random;
        private static final String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz123456789";
        private String[] randomStrings;

        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
        }

        public String generate() {
            return randomStrings[random.nextInt(RANDOM_STRINGS)];
        }

        public Attribute(int length) {
            this.length = length;
            this.random = new Random();

            randomStrings = new String[RANDOM_STRINGS];

            // Generate RANDOM_STRINGS random strings because it is faster
            for (int i = 0; i < randomStrings.length; i++) {
                char[] buf = new char[getLength()];
                for (int j = 0; j < getLength(); j++) {
                    buf[j] = chars.charAt(random.nextInt(chars.length()));
                }

                randomStrings[i] = new String(buf);
            }
        }
    }

    enum KeyType {
        NUMERIC, STRING
    }

    private int nRows;
    private List<Attribute> attributes;
    private KeyType keyType;
    private int keyRepetitions;

    public DataGenerator(KeyType keyType, int nRows, List<Attribute> attributes, int keyRepetitions) {
        this.keyType = keyType;
        this.nRows = nRows;
        this.attributes = attributes;
        this.keyRepetitions = keyRepetitions;
    }

    public void write(DataOutputStream file1, DataOutputStream file2) {
        PrintWriter t1writer = new PrintWriter(file1);
        PrintWriter t2writer = new PrintWriter(file2);

        int keyModulo = nRows / keyRepetitions;

        for (int i = 0; i < nRows; i++) {
            String row = "" + (i % keyModulo);

            for (Attribute a : attributes) {
                row += "," + a.generate();
            }

            row += "\n";

            t1writer.write(row);
            t2writer.write(row);
        }

        t1writer.close();
        t2writer.close();
    }

    public void writeZipf(DataOutputStream file1, DataOutputStream file2, double s) {
        PrintWriter t1writer = new PrintWriter(file1);
        PrintWriter t2writer = new PrintWriter(file2);

        Integer[] keys = new Integer[nRows];
        for (int i = 0; i < nRows; i++) {
            keys[i] = i;
        }

        List<Integer> keysList = new ArrayList<Integer>(Arrays.asList(keys));
        Collections.shuffle(keysList);

        for (int i = 0; i < nRows; i++) {
            String row = "" + (keysList.get(i));

            for (Attribute a : attributes) {
                row += "," + a.generate();
            }

            row += "\n";

            t1writer.write(row);
        }

        for (int i = 0; i < nRows; i++) {
            String row = "" + zipfInverseCdf((double)i / (double)nRows, s, (double) nRows / keyRepetitions);

            for (Attribute a : attributes) {
                row += "," + a.generate();
            }

            row += "\n";

            t2writer.write(row);
        }

        t1writer.close();
        t2writer.close();
    }

    public void writeZipfBoth(DataOutputStream file1, DataOutputStream file2, double s) {
        PrintWriter t1writer = new PrintWriter(file1);
        PrintWriter t2writer = new PrintWriter(file2);

        for (int i = 0; i < nRows; i++) {
            String row = "" + zipfInverseCdf((double)i / (double)nRows, s, (double) nRows / keyRepetitions);

            for (Attribute a : attributes) {
                row += "," + a.generate();
            }

            row += "\n";

            t1writer.write(row);
            t2writer.write(row);
        }

        t1writer.close();
        t2writer.close();
    }

    /**
     * A quick way to test the data generator's performance
     * @param args
     */
    public static void main(String[] args) throws IOException {
        int rowsStep = 1000000;
        int repetitions = 10;

        for (int i = 1; i <= 6; i++) {
            int nRows = i * rowsStep;

            DataGenerator dg = new DataGenerator(DataGenerator.KeyType.NUMERIC, nRows,
                    Arrays.asList(new DataGenerator.Attribute(20), new DataGenerator.Attribute(100),
                            new DataGenerator.Attribute(80)), repetitions);

            File input1 = new File("t1_" + nRows + ".csv");
            File input2 = new File("t2_" + nRows + ".csv");

            DataOutputStream out1 = new DataOutputStream(new FileOutputStream(input1));
            DataOutputStream out2 = new DataOutputStream(new FileOutputStream(input2));

            long startTime = System.nanoTime();
            dg.writeZipf(out1, out2, 0.5);
            long endTime = System.nanoTime();

            long diff = endTime - startTime;

            out1.close();
            out2.close();

            System.out.printf("Data generated(nrows=%d): %.3f ms - file size: %dMB\n",
                    nRows, diff / 1000000.0, input1.length() / 1000000);

            input1.delete();
            input2.delete();
        }
    }

    /**
     * An approximation of the inverse CDF of the Zipf distribution
     * Source: https://medium.com/@jasoncrease/zipf-54912d5651cc
     * @param p Probability
     * @param s Skew parameter: 0 = no skew, 1 ~ skew of the English language
     * @param N The number of elements
     * @return The value
     */
    private static long zipfInverseCdf(final double p, final double s, final double N) {
        if (p > 1d || p < 0d)
            throw new IllegalArgumentException("p must be between 0 and 1");

        final double tolerance = 0.01d;
        double x = N / 2;

        final double D = p * (12 * (Math.pow(N, 1 - s) - 1) / (1 - s) + 6 - 6 * Math.pow(N, -s) + s - Math.pow(N, -1 - s) * s);

        while (true) {
            final double m    = Math.pow(x, -2 - s);
            final double mx   = m   * x;
            final double mxx  = mx  * x;
            final double mxxx = mxx * x;

            final double a = 12 * (mxxx - 1) / (1 - s) + 6 * (1 - mxx) + (s - (mx * s)) - D;
            final double b = 12 * mxx + 6 * (s * mx) + (m * s * (s + 1));
            final double newx = Math.max(1, x - a / b);
            if (Math.abs(newx - x) <= tolerance)
                return (long) newx;
            x = newx;
        }
    }
}
