import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.i
import org.apache.spark.sql.sources.In;
import org.codehaus.janino.Java;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import scala.Tuple2;
import shapeless.Tuple;

import java.io.*;
import java.util.*;
import java.util.function.BiConsumer;

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

    public static List<Object> find_points_in_dist_d(double d, Object point,JavaRDD<Object> points_rdd) {

        return points_rdd.filter(new Function<Object, Boolean>() {
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
        }).collect();
    }

    public static List<Object> find_points_in_dist_d(double d, Object point,List<Object> points_rdd) {

        List<Object> ans=new ArrayList<>();
        for(Object o:points_rdd)
        {
            if(!o.equalsTo(point))
            {
                if(calca_dist(o,point)<=d)
                {
                    ans.add(o);
                }
            }
        }
        return ans;
    }
    public static HashMap<Object, HashMap<String,Double>> FractionComputation(JavaRDD<Object> points_rdd, JavaRDD<String> spatial_feature_rdd, double dist_thresh) {

//        List<Object> allPoints=points_rdd.collect();
//        HashMap<Object,List<Object>> map=new HashMap<>();
//        HashMap<Object, List<String>> neighbour_set=new HashMap<>();
//
//        for(Object object:allPoints)
//        {
//            List<Object> pointsInDistD=find_points_in_dist_d(dist_thresh,object,points_rdd).collect();
//            map.put(object,pointsInDistD);
//            List<String> labelInDistD=new ArrayList<>();
//            for(Object o:pointsInDistD)
//            {
//                labelInDistD.add(o.event_type);
//            }
//            neighbour_set.put(object,labelInDistD);
//        }
//
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
            List<Object> list_of_points_in_d=find_points_in_dist_d(dist_thresh,o,allSpatialPoints);
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
                double obj=1D/v;
//                System.out.println(v+" "+obj);

                label_set.get(o_dash).replace(str,label_set.get(o_dash).get(str),label_set.get(o_dash).get(str)+obj);
                if(label_set.get(o_dash).get(str)>1)
                {
                    label_set.get(o_dash).replace(str,1D);
                }
            }
        }

        return label_set;


//        return points_rdd.mapToPair(new PairFunction<Object, Object, JavaPairRDD<String, Double>>() {
//            @Override
//            public Tuple2<Object, JavaPairRDD<String, Double>> call(Object object) throws Exception {
//                //object==D1
//                String type=object.event_type;
//                List<Object> pointsInDistD=new ArrayList<>();
//                for(Object point:map.keySet())
//                {
//                    if(point.event_type.equals(type) && point.instance_id==object.instance_id)
//                    {
//                        pointsInDistD=map.get(point);
//                        break;
//                    }
//                }
//                HashMap<String,Long> eventTypeRdd=new HashMap<>();
//
//                for(Object o:pointsInDistD)
//                {
//                    eventTypeRdd.put(o.event_type,0L);
//                }
//                for(Object o:pointsInDistD)
//                {
//
//                }
//                        pointsInDistD.mapToPair(new PairFunction<Object,String, Long>() {
//
//
//                    @Override
//                    public Tuple2<String, Long> call(Object o) throws Exception {
//                        long count=neighbour_set_rdd.map(new Function<Tuple2<Object, JavaRDD<String>>, JavaRDD<String>>() {
//
//                            @Override
//                            public JavaRDD<String> call(Tuple2<Object, JavaRDD<String>> tuple2) throws Exception {
//                                if(o.event_type.equals(tuple2._1.event_type) && o.instance_id==tuple2._1.instance_id)
//                                {
//                                    return tuple2._2;
//                                }
//                                return null;
//                            }
//                        }).first().filter(new Function<String, Boolean>() {
//                            @Override
//                            public Boolean call(String s) throws Exception {
//                                return s.equals(type);
//                            }
//                        }).count();
//
//                        return new Tuple2<>(o.event_type, count);
//                    }
//                }).reduceByKey(Long::sum);
//
//
//
//                JavaPairRDD<String,Double> labelOfEachEvent=eventTypeRdd.mapToPair(new PairFunction<Tuple2<String, Long>, String, Double>() {
//                    @Override
//                    public Tuple2<String, Double> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
//                        Double obj=1D/stringLongTuple2._2;
//                        return new Tuple2<>(stringLongTuple2._1,obj) ;
//                    }
//                });
//
//
//                return new Tuple2<>(object,labelOfEachEvent);
//            }
//        });

    }

    public static Double FractionAggregation(JavaRDD<String> candidateColocationRdd, Object o, JavaRDD<Object> allSpatialObjects, JavaPairRDD<Object, List<Tuple2<String, Double>>> label_set_rdd) {
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


        List<Tuple2<String, Double>> labelsForObjectO=new ArrayList<>();
        for(Tuple2<Object,List<Tuple2<String,Double>>> t:label_set_rdd.collect())
        {
            if(o.equalsTo(t._1))
            {
                labelsForObjectO=new ArrayList<>(t._2);
            }
        }
        double minLabelSet=Double.MAX_VALUE;
        for(Tuple2<String, Double> t:labelsForObjectO)
        {
            if(!t._1.equals(label))
            {
                minLabelSet=Math.min(minLabelSet,t._2);
            }
        }

        return minLabelSet;
//        return labelSetValue;

        //iterating over the labelSet,i.e., the candidate co-location

    }

    static boolean CombinatorialSearch(Object o, JavaRDD<Object> points_rdd, double dist_thresh, JavaRDD<String> candidateColocation,JavaSparkContext sc)
    {
        //colocation without label type of object o
        JavaRDD<String> colocation=candidateColocation.filter(s -> !s.equals(o.event_type));
        HashSet<String> set = new HashSet<>(colocation.collect());
        String event=candidateColocation.first();

        //points in dist d of object o
        JavaRDD<Object> pointsInDistD=sc.parallelize(find_points_in_dist_d(dist_thresh/2,o,points_rdd));

        JavaRDD<Object> firstEventTypeObjects=pointsInDistD.filter(new Function<Object, Boolean>() {
            @Override
            public Boolean call(Object object) throws Exception {
                return object.event_type.equals(event);
            }
        });

        JavaRDD<Object> pointsInDistDInColoc=pointsInDistD.filter(new Function<Object, Boolean>() {
            @Override
            public Boolean call(Object object) throws Exception {
                if(set.contains(object.event_type))
                {
                    if(object.event_type.equals(event))
                    {
                        return false;
                    }
                    return true;
                }
                else
                {
                    return false;
                }
            }
        });


//        JavaRDD<Object> pointsInDistD2=find_points_in_dist_d(dist_thresh/2,o,points_rdd);
        //pair list that seperates all objects in Disk(o,d) by their event type
        JavaPairRDD<String,JavaRDD<Object>> pairEventObject=colocation.mapToPair(new PairFunction<String, String, JavaRDD<Object>>() {
            @Override
            public Tuple2<String, JavaRDD<Object>> call(String s) throws Exception {
                JavaRDD<Object> rddOfEventS=pointsInDistDInColoc.filter(object-> object.event_type.equals(s));
                return new Tuple2<>(s,rddOfEventS);
            }
        });


        JavaRDD<JavaRDD<Object>> cartesianOfRDD=pairEventObject.map(new Function<Tuple2<String, JavaRDD<Object>>, JavaRDD<Object>>() {

            @Override
            public JavaRDD<Object> call(Tuple2<String, JavaRDD<Object>> stringJavaRDDTuple2) throws Exception {
                return stringJavaRDDTuple2._2;
            }
        });



        HashSet<String> labels=new HashSet<>();
        pointsInDistD.foreach(new VoidFunction<Object>() {
            @Override
            public void call(Object object) throws Exception {
                labels.add(object.event_type);
            }
        });
        if(labels.size()==pointsInDistD.count())
        {
            return true;
        }
        else
        {
            return false;

        }
//                .foreach(new VoidFunction<JavaRDD<Object>>() {
//            @Override
//            public void call(JavaRDD<Object> objectJavaRDD) throws Exception {
//                firstEventTypeObjects.cartesian(objectJavaRDD);
//            }
//        })



    }


    static Double SupportComputation(List<String> labelSet, JavaRDD<Object> allSpatialObjects, JavaPairRDD<Object, List<Tuple2<String, Double>>> label_set_rdd,double dist_thresh,JavaSparkContext sc) {
        List<Object> allSpatialPoints = allSpatialObjects.collect();
        double minSup = Double.MAX_VALUE;
        for (String label : labelSet) {
            double sup = 0;
            for (Object o : allSpatialPoints) {
                if (CombinatorialSearch(o,allSpatialObjects,dist_thresh,sc.parallelize(labelSet),sc)) {
                    sup += FractionAggregation(sc.parallelize(labelSet), o, allSpatialObjects, label_set_rdd);
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

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("BDAProject")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        /*reading data from data.txt*/
        JavaRDD<String> lines = sc.textFile("/Users/gagan/Downloads/BTP_Data/sample.txt");
//                "../../../../../Downloads/BTP_Data/sample.txt");

        /*creating objects using create_Object function*/
        JavaRDD<Object> allSpatialObjects = lines.map(FractionScore::create_Object);
        /*
        printing all objects
         */
//        allSpatialObjects.foreach(new VoidFunction<Object>() {
//            @Override
//            public void call(Object object) throws Exception {
//                System.out.println(object.event_type +" "+ object.instance_id + " " + object.x + " " + object.y);
//            }
//        });
//
//
//        System.out.println("all event types");
        JavaRDD<String> allEventTypes = allSpatialObjects.map(new Function<Object, String>() {
            @Override
            public String call(Object object) throws Exception {
                return object.event_type;
            }
        }).distinct();
//        allEventTypes.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//
//            }
//        });

        HashMap<Object, HashMap<String,Double>> labelSetRdd=FractionComputation(allSpatialObjects,allEventTypes,0.3);
        List<Tuple2<Object,List<Tuple2<String,Double>>>> fractionComp=new ArrayList<>();

        for(Object o:labelSetRdd.keySet())
        {
            HashMap<String,Double> map=labelSetRdd.get(o);
            List<Tuple2<String,Double>> list=new ArrayList<>();
            for(String s:map.keySet())
            {
                Tuple2<String,Double> t=new Tuple2<String,Double>(s,map.get(s));
                list.add(t);
            }
            Tuple2<Object,List<Tuple2<String,Double>>> tuple2=new Tuple2<Object,List<Tuple2<String,Double>>>(o,list);
            fractionComp.add(tuple2);
        }
        JavaPairRDD<Object, List<Tuple2<String, Double>>> fractionForEachLabel=sc.parallelizePairs(fractionComp);


        BufferedReader in=null;
        try
        {
            in=new BufferedReader(new FileReader("/Users/gagan/Downloads/BTP_Data/Candidate_Colocations.txt"));

            String l;
            while((l=in.readLine())!=null)
            {
                List<String> list=new ArrayList<>(Arrays.asList(l.split(" ")));
                double support=SupportComputation(list,allSpatialObjects,fractionForEachLabel,0.3,sc);
                if(support>0.3)
                {
                    System.out.println(l+" is a colocation => "+support);
                }
                else {
                    System.out.println(l+" is not a colocation => "+support);
                }
            }

        }
        finally {
            if(in!=null)
            {
                in.close();
            }
        }

    }
}
