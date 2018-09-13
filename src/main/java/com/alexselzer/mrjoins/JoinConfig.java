package com.alexselzer.mrjoins;

import org.apache.hadoop.fs.Path;

public class JoinConfig {
    private Path[] inputs;
    private Integer[] indices;
    private Path output;
    private int numReducers = 1;

    public JoinConfig(Path[] inputs, Integer[] indices, Path output, int numReducers) {
        this.inputs = inputs;
        this.indices = indices;
        this.output = output;
        this.numReducers = numReducers;
    }

    public Path[] getInputs() {
        return inputs;
    }

    public void setInputs(Path[] inputs) {
        this.inputs = inputs;
    }

    public Integer[] getIndices() {
        return indices;
    }

    public void setIndices(Integer[] indices) {
        this.indices = indices;
    }

    public Path getOutput() {
        return output;
    }

    public void setOutput(Path output) {
        this.output = output;
    }

    public int getNumReducers() {
        return numReducers;
    }

    public void setNumReducers(int numReducers) {
        this.numReducers = numReducers;
    }
}
