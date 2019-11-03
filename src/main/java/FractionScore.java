import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import org.codehaus.janino.Java;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import scala.Tuple2;
import shapeless.Tuple;

import java.io.DataOutput;
import java.io.Serializable;
import java.util.*;
import java.util.function.BiConsumer;

class Spatial_Feature implements Serializable {
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
    private int x, y;
//    private String name;

    public Spatial_Point(Spatial_Feature feature_type, int x, int y) {
        this.feature_type = feature_type;
        this.x = x;
        this.y = y;
    }

    public Spatial_Point(int x, int y) {
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
    private static double calca_dist(Object s1, Object s2) {
        double diff_x = Math.pow(s1.getX() - s2.getX(), 2);
        double diff_y = Math.pow(s1.getY() - s2.getY(), 2);

        return Math.sqrt(diff_x + diff_y);
    }

    static Comparator<Double> increasing=new Comparator<Double>() {
        @Override
        public int compare(Double o1, Double o2) {
            if(o1-o2<0)
            {
                return -1;
            }else if(o1-o2>0)
            {
                return 1;
            }
            else {
                return 0;
            }
        }
    };

    public static JavaRDD<Object> find_points_in_dist_d(double d, Object point,JavaRDD<Object> points_rdd) {

        JavaRDD<Object> pointsInDistD=points_rdd.filter(new Function<Object, Boolean>() {
            @Override
            public Boolean call(Object object) throws Exception {
                if(object.event_type.equals(point.event_type) && object.instance_id==point.instance_id)
                {
                    return false;
                }
                if(calca_dist(point,object)<=d)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        });
        return pointsInDistD;
    }

    public static JavaPairRDD<Object, JavaPairRDD<String, Double>> FractionComputation(JavaRDD<Object> points_rdd, JavaRDD<String> spatial_feature_rdd, double dist_thresh) {

        JavaPairRDD<Object, JavaRDD<String>> neighbour_set_rdd=points_rdd.mapToPair(new PairFunction<Object, Object, JavaRDD<String>>() {

            @Override
            public Tuple2<Object, JavaRDD<String>> call(Object object) throws Exception {
                JavaRDD<Object> pointsInDistD=find_points_in_dist_d(dist_thresh,object,points_rdd);

                JavaRDD<String> countOfEachEventType=pointsInDistD.map(x->x.event_type);
                return new Tuple2<>(object,countOfEachEventType);
            }
        });


        JavaPairRDD<Object, JavaPairRDD<String, Double>> label_set_rdd=points_rdd.mapToPair(new PairFunction<Object, Object, JavaPairRDD<String, Double>>() {
            @Override
            public Tuple2<Object, JavaPairRDD<String, Double>> call(Object object) throws Exception {
                //object==D1
                String type=object.event_type;

                JavaRDD<Object> pointsInDistD=find_points_in_dist_d(dist_thresh,object,points_rdd);
                JavaPairRDD<String,Long> eventTypeRdd=pointsInDistD.mapToPair(new PairFunction<Object,String, Long>() {


                    @Override
                    public Tuple2<String, Long> call(Object o) throws Exception {
                        long count=neighbour_set_rdd.map(new Function<Tuple2<Object, JavaRDD<String>>, JavaRDD<String>>() {

                            @Override
                            public JavaRDD<String> call(Tuple2<Object, JavaRDD<String>> tuple2) throws Exception {
                                if(o.event_type.equals(tuple2._1.event_type) && o.instance_id==tuple2._1.instance_id)
                                {
                                    return tuple2._2;
                                }
                                return null;
                            }
                        }).first().filter(new Function<String, Boolean>() {
                            @Override
                            public Boolean call(String s) throws Exception {
                                return s.equals(type);
                            }
                        }).count();

                        return new Tuple2<>(o.event_type, count);
                    }
                }).reduceByKey(Long::sum);



                JavaPairRDD<String,Double> labelOfEachEvent=eventTypeRdd.mapToPair(new PairFunction<Tuple2<String, Long>, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        Double obj=1D/stringLongTuple2._2;
                        return new Tuple2<>(stringLongTuple2._1,obj) ;
                    }
                });


                return new Tuple2<>(object,labelOfEachEvent);
            }
        });
        return label_set_rdd;

    }

    public static Double FractionAggregation(JavaRDD<String> candidateColocationRdd, Object o, JavaRDD<Object> allSpatialObjects, JavaPairRDD<Object, JavaPairRDD<String, Double>> label_set_rdd) {
        String label = o.event_type;

//        double labelSetValue = Double.MAX_VALUE;
        HashSet<String> setOfDistinctLabels=new HashSet<>();
        candidateColocationRdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if(s.equals(label))
                {
                    return false;
                }
                else {
                    return true;
                }

            }
        }).foreach((VoidFunction<String>) s -> setOfDistinctLabels.add(s));

        JavaPairRDD<String,Double> labelsForObjectO=label_set_rdd.filter(tuple2 -> o.equalsTo(tuple2._1)).values().first();
        return labelsForObjectO.filter( tuple2 -> {
            if(label.equals(tuple2._1))
            {
                return false;
            }
            else {
                return true;
            }
        }).map(t->t._2).min(increasing);

//        return labelSetValue;

        //iterating over the labelSet,i.e., the candidate co-location

    }

    static Double SupportComputation(ArrayList<String> labelSet, JavaRDD<Object> allSpatialObjects, HashMap<Object, HashMap<String, Double>> label_set_rdd) {
        List<Object> allSpatialPoints = allSpatialObjects.collect();
        double minSup = Double.MAX_VALUE;
        for (String label : labelSet) {
            double sup = 0;
            for (Object o : allSpatialPoints) {
                if (/*RI*/true) {
//                    sup += FractionAggregation(labelSet, o, allSpatialObjects, label_set_rdd);
                }
            }
            if (minSup < sup) {
                minSup = sup;
            }
        }
        return minSup;

    }

    public static Object create_Object(String line) {
        String values[] = line.split(" ");
        Object o = new Object(values[0], Integer.parseInt(values[1]), Double.parseDouble(values[2]), Double.parseDouble(values[3]));
        return o;
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
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
                System.out.println(object.event_type + object.instance_id + " " + object.x + " " + object.y);
            }
        });


        System.out.println("all event types");
        JavaRDD<String> allEventTypes = allSpatialObjects.map(new Function<Object, String>() {
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
//        HashMap<Object, HashMap<String, Double>> label_set_rdd = FractionComputation(allSpatialObjects, allEventTypes, 0.2);
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
//        label_set_rdd.forEach(new BiConsumer<Object, HashMap<String, Double>>() {
//            @Override
//            public void accept(Object object, HashMap<String, Double> stringDoubleHashMap) {
//                System.out.println(object.event_type + object.instance_id);
//                for (Map.Entry<String, Double> m : stringDoubleHashMap.entrySet()) {
//                    System.out.println("\t" + m.getKey() + " " + m.getValue());
//                }
//            }
//        });


    }
}
