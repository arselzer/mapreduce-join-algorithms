package com.alexselzer.mrjoins;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class DataGenerator {
    public static class Attribute {
        private int length;
        private Random random;
        private static final String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
        }

        public String generate() {
            char[] buf = new char[getLength()];
            for (int i = 0; i < getLength(); i++) {
                buf[i] = chars.charAt(random.nextInt(chars.length()));
            }

            return new String(buf);
        }

        public Attribute(int length) {
            this.length = length;
            this.random = new Random();
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
}
