package com.abinj.sparkjava.models;

import java.io.Serializable;

public class AvgCount implements Serializable {
    private int total_;
    private int num_;
    public AvgCount(int total, int num) {
        total_ = total;
        num_ = num;
    }

    public float avg() {
        return total_/(float)num_;
    }

    public int getTotal_() {
        return total_;
    }

    public void setTotal_(int total_) {
        this.total_ = total_;
    }

    public int getNum_() {
        return num_;
    }

    public void setNum_(int num_) {
        this.num_ = num_;
    }
}
