package edu.rosehulman.goistjt;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Ratio extends EvalFunc<Double> {
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcSpecs = new ArrayList<>();
        Schema tupleSchema = new Schema();
        tupleSchema.add(new Schema.FieldSchema(null, DataType.LONG));
        tupleSchema.add(new Schema.FieldSchema(null, DataType.LONG));
        funcSpecs.add(new FuncSpec(this.getClass().getName(), tupleSchema));
        return funcSpecs;
    }

    @Override
    public Double exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0) {
            return null;
        }

        if (tuple.get(0) == null || tuple.get(1) == null) {
            return null;
        }

        return ((Long) tuple.get(0)).doubleValue() / ((Long) tuple.get(1)).doubleValue();
    }
}
