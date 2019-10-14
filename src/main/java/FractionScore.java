import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import scala.Tuple2;
import shapeless.Tuple;

import java.io.DataOutput;
import java.io.Serializable;
import java.util.*;
import java.util.function.BiConsumer;

class Spatial_Feature implements Serializable{
    private String feature_name;

    public Spatial_Feature(String feature_name) {
        this.feature_name = feature_name;
    }

    public String getFeature_name() {
        return feature_name;
    }

    public void setFeature_name(String feature_name) {
        this.feature_name = feature_name;
    }

}
class Spatial_Point implements Serializable {
    private Spatial_Feature feature_type;
    private int x,y;
//    private String name;

    public Spatial_Point(Spatial_Feature feature_type, int x, int y) {
        this.feature_type = feature_type;
        this.x = x;
        this.y = y;
    }
    public Spatial_Point( int x, int y) {
        this.x = x;
        this.y = y;
    }
    public Spatial_Feature getFeature_type() {
        return feature_type;
    }

    public void setFeature_type(Spatial_Feature feature_type) {
        this.feature_type = feature_type;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }
}
public class FractionScore {
    private static double calca_dist(Object s1,Object s2)
    {
        double diff_x=Math.pow(s1.getX()-s2.getX(),2);
        double diff_y=Math.pow(s1.getY()-s2.getY(),2);

        return Math.sqrt(diff_x+diff_y);
    }
    public static LinkedList<Object> find_points_in_dist_d(double d,Object point,HashMap<Object, HashMap<String,Integer>> neighbour_count_map)
    {
        LinkedList<Object> list_of_points_in_d=new LinkedList<>();
        for(Object o:neighbour_count_map.keySet())
        {
            if(o.equals(point))
            {
                continue;
            }
            else
            {
                if(calca_dist(o,point)<=d)
                {
                    list_of_points_in_d.add(o);
                }
            }
        }

        return list_of_points_in_d;
    }
    public static HashMap<Object, HashMap<String,Double>> FractionComputation(JavaRDD<Object> points_rdd,JavaRDD<String> spatial_feature_rdd, double dist_thresh)
    {
        List<Object> allSpatialPoints=points_rdd.collect();
        List<String> allSpatialFeatures=spatial_feature_rdd.collect();
        HashMap<Object, HashMap<String,Integer>> neighbour_count_map=new HashMap<>();
        HashMap<Object, HashMap<String,Double>> label_set=new HashMap<>();
//        JavaPairRDD<Spatial_Point, HashMap<Spatial_Feature,Double>> label_set_rdd;
        for (Object p:allSpatialPoints) {
            HashMap<String,Integer> map_neigh=new HashMap<>();
            HashMap<String,Double> map_label=new HashMap<>();
            for (String f:allSpatialFeatures)
            {
                map_neigh.put(f,0);
                map_label.put(f,0D);

            }
            neighbour_count_map.put(p,map_neigh);
            label_set.put(p,map_label);
//            System.out.println("Neighbour map" + neighbour_count_map.get(p).size());
        }
//        System.out.println(neighbour_count_map.size());
//        for (Object p:points_rdd.collect()) {
//
//            System.out.println("Neighbour map" + neighbour_count_map.get(p).size());
//        }
//        System.out.println("points");
//        for (Object p:allSpatialPoints) {
//            System.out.println(p);
//        }
//        System.out.println("neigh");
//        for(Map.Entry m:neighbour_count_map.entrySet())
//        {
//            System.out.println(m.getKey());
//        }

        for (Object o:allSpatialPoints) {
            LinkedList<Object> list_of_points_in_d=find_points_in_dist_d(dist_thresh,o,neighbour_count_map);
//            System.out.println("val="+list_of_points_in_d.size());
            for(Object o_dash:list_of_points_in_d)
            {
                String str=o_dash.event_type;
                int val=neighbour_count_map.get(o).get(str);
                neighbour_count_map.get(o).replace(str,val+1);
            }


            for(Object o_dash:list_of_points_in_d)
            {
                String str=o.event_type;
                int v=neighbour_count_map.get(o).get(str);
                double obj=1D/neighbour_count_map.get(o).get(str);
//                System.out.println(v+" "+obj);

                label_set.get(o_dash).replace(str,label_set.get(o_dash).get(str),label_set.get(o_dash).get(str)+obj);
                if(label_set.get(o_dash).get(str)>1)
                {
                    label_set.get(o_dash).replace(str,1D);
                }
            }
        }

        return label_set;

    }

    public static Double FractionAggregation(ArrayList<String> labelSet, Object o, JavaRDD<Object> allSpatialObjects ,HashMap<Object, HashMap<String,Double>> label_set_rdd)
    {
        String label=o.event_type;

        double labelSetValue=Double.MAX_VALUE;

        //iterating over the labelSet,i.e., the candidate co-location
        for(String str:labelSet)
        {
            if(!str.equals(label))
            {
                double val=label_set_rdd.get(o).get(str);
                if(val<labelSetValue)
                {
                    labelSetValue=val;
                }
            }
        }
        return labelSetValue;
    }

    static Double SupportComputation(ArrayList<String> labelSet, JavaRDD<Object> allSpatialObjects, HashMap<Object, HashMap<String,Double>> label_set_rdd )
    {
        List<Object> allSpatialPoints=allSpatialObjects.collect();
        double minSup= Double.MAX_VALUE;
        for(String label:labelSet)
        {
            double sup=0;
            for(Object o:allSpatialPoints)
            {
                if(/*RI*/true)
                {
                    sup+=FractionAggregation(labelSet,o,allSpatialObjects,label_set_rdd);
                }
            }
            if(minSup<sup)
            {
                minSup=sup;
            }
        }
        return minSup;

    }
    public static Object create_Object(String line)
    {
        String values [] = line.split(" ");
        Object o = new Object(values[0],Integer.parseInt(values[1]), Double.parseDouble(values[2]), Double.parseDouble(values[3]) );
        return o;
    }

    public static void main(String[] args)
    {
        SparkSession spark= SparkSession.builder()
                .master("local")
                .appName("BDAProject")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        /*reading data from data.txt*/
        JavaRDD<String> lines = sc.textFile("data.txt");

        /*creating objects using create_Object function*/
        JavaRDD<Object> allSpatialObjects = lines.map(FractionScore::create_Object);
        /*
        printing all objects
         */
        allSpatialObjects.foreach(new VoidFunction<Object>() {
            @Override
            public void call(Object object) throws Exception {
                System.out.println(object.event_type+object.instance_id+" "+object.x+" "+object.y);
            }
        });


        System.out.println("all event types");
        JavaRDD<String> allEventTypes=allSpatialObjects.map(new Function<Object, String>() {
            @Override
            public String call(Object object) throws Exception {
                return object.event_type;
            }
        }).distinct();
        allEventTypes.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
//        System.out.println("LABELS RDD");
        HashMap<Object, HashMap<String,Double>> label_set_rdd=FractionComputation(allSpatialObjects,allEventTypes,0.2);
//        label_set_rdd.foreach(new VoidFunction<Tuple2<Spatial_Point, HashMap<String, Double>>>() {
//            @Override
//            public void call(Tuple2<Spatial_Point, HashMap<String, Double>> spatial_pointHashMapTuple2) throws Exception {
//                Spatial_Point p=spatial_pointHashMapTuple2._1;
//                HashMap<String, Double> map=spatial_pointHashMapTuple2._2;
//                System.out.println(p.getFeature_type().getFeature_name()+" "+p.getX()+" "+p.getY());
//                for(Map.Entry m:map.entrySet())
//                {
////                    String f= (Spatial_Feature) m.getKey();
//                    System.out.println("   "+m.getKey()+" "+m.getValue());
//                }
//            }
//        });
        label_set_rdd.forEach(new BiConsumer<Object, HashMap<String, Double>>() {
            @Override
            public void accept(Object object, HashMap<String, Double> stringDoubleHashMap) {
                System.out.println(object.event_type+object.instance_id);
                for(Map.Entry<String,Double> m:stringDoubleHashMap.entrySet())
                {
                    System.out.println("\t"+m.getKey()+" "+m.getValue());
                }
            }
        });


    }
}
