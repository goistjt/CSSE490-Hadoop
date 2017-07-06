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

public class Upper extends EvalFunc<String> {

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcSpecs = new ArrayList<>();
        funcSpecs.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
        return funcSpecs;
    }

    public String exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() < 1) {
            return null;
        }

        return ((String) tuple.get(0)).toUpperCase();
    }
}
