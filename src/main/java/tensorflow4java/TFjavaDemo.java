package tensorflow4java;

import org.tensorflow.Graph;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import org.tensorflow.Tensors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by Joe.Kwan on 2020/1/7
 */

public class TFjavaDemo {

    public static void main(String[] args) {
        byte[] graphDef = loadTensorflowModel("/Users/yueguan/Downloads/models/demoTest/rf.pb");


        float inputs[][] = new float[4][6];
        for (int i= 0; i< 4; i++) {
            for (int j= 0; j< 6; j++) {
                if (i< 2) {
                    inputs[i][j] = 2* i - 5* j -6;
                }
                else {
                    inputs[i][j] = 2* i + 5* j -6;
                }
            }
        }

//        Tensor<Float> input = Tensor.create(inputs, Float.class);
        Tensor<Float> input = covertArrayToTensor(inputs);

        Graph g = new Graph();
        g.importGraphDef(graphDef);

        Session s = new Session(g);
        Tensor result = s.runner().feed("input_data", input).fetch("output_data").run().get(0);

        long[] rshape = result.shape();
        int rs = (int) rshape[0];
        long realResult[] = new long[rs];
        result.copyTo(realResult);

        for (long a: realResult) {
            System.out.println(a);
        }

    }


    static private byte[] loadTensorflowModel(String path) {
        try {
            return Files.readAllBytes(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    static private Tensor<Float> covertArrayToTensor(float input[][]){
        return Tensors.create(input);
    }

}
