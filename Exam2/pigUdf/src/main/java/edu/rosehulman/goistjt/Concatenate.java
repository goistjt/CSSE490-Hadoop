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

public class Concatenate extends EvalFunc<String> {
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcSpecs = new ArrayList<>();
        Schema tupleSchema = new Schema();
        tupleSchema.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        tupleSchema.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcSpecs.add(new FuncSpec(this.getClass().getName(), tupleSchema));
        return funcSpecs;
    }

    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() != 2) return null;
        if (input.get(0) == null || input.get(1) == null) return null;
        return ((String) input.get(0)).trim() + " " + ((String) input.get(1)).trim();
    }
}
