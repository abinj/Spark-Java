package com.abinj.sparkjava.models;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class Slots {
    public static final StructType SCHEMA;

    static {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        fields.add(DataTypes.createStructField("slot_id", DataTypes.LongType, false));
        fields.add(DataTypes.createStructField("slot_type_id", DataTypes.LongType, false));
        fields.add(DataTypes.createStructField("completion_reason", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("entry_timestamp", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("exit_timestamp", DataTypes.StringType, true));

        SCHEMA = DataTypes.createStructType(fields);
    }
}
