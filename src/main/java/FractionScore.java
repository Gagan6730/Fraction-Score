import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;
import shapeless.Tuple;

import java.io.Serializable;
import java.util.*;

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
    private static double calca_dist(Spatial_Point s1,Spatial_Point s2)
    {
        double diff_x=Math.pow(s1.getX()-s2.getX(),2);
        double diff_y=Math.pow(s1.getY()-s2.getY(),2);

        return Math.sqrt(diff_x+diff_y);
    }
    public static LinkedList<Spatial_Point> find_points_in_dist_d(int d,Spatial_Point point,List<Tuple2<Spatial_Point, HashMap<Spatial_Feature,Integer>>> neighbour_count_map)
    {
        LinkedList<Spatial_Point> list_of_points_in_d=new LinkedList<>();
        for (int i=0;i<neighbour_count_map.size();i++)
        {
            if(neighbour_count_map.get(i)._1().equals(point))
            {
                continue;
            }
            else
            {
                if(calca_dist(neighbour_count_map.get(i)._1(),point)>=d)
                {
                    list_of_points_in_d.add(neighbour_count_map.get(i)._1());
                }
            }
        }
        return list_of_points_in_d;
    }
    public static List<Tuple2<Spatial_Point, HashMap<Spatial_Feature,Double>>> FractionComputation(JavaRDD<Spatial_Point> points_rdd,JavaRDD<Spatial_Feature> spatial_feature_rdd, int dist_thresh)
    {
        List<Tuple2<Spatial_Point, HashMap<Spatial_Feature,Integer>>> neighbour_count_map=new LinkedList<>();
        List<Tuple2<Spatial_Point, HashMap<Spatial_Feature,Double>>> label_set=new LinkedList<>();
//        JavaPairRDD<Spatial_Point, HashMap<Spatial_Feature,Double>> label_set_rdd;
        for (Spatial_Point p:points_rdd.collect()) {
            HashMap<Spatial_Feature,Integer> map_neigh=new HashMap<>();
            HashMap<Spatial_Feature,Double> map_label=new HashMap<>();
            for (Spatial_Feature f:spatial_feature_rdd.collect())
            {
                map_neigh.put(f,0);
                map_label.put(f,0D);

            }
            neighbour_count_map.add(new Tuple2<>(p,map_neigh));
            label_set.add(new Tuple2<>(p,map_label));
        }
//        for (int i=0;i<neighbour_count_map.size();i++) {
//            Tuple2<Spatial_Point, HashMap<Spatial_Feature, Integer>> spatial_pointHashMapTuple2=neighbour_count_map.get(i);
//            Spatial_Point p=spatial_pointHashMapTuple2._1;
//            HashMap<Spatial_Feature, Integer> map=spatial_pointHashMapTuple2._2;
//            System.out.println(p.getFeature_type().getFeature_name()+" "+p.getX()+" "+p.getY());
//            for(Map.Entry m:map.entrySet())
//            {
//                Spatial_Feature f= (Spatial_Feature) m.getKey();
//                System.out.println("   "+f.getFeature_name()+" "+m.getValue());
//            }
//        }

        for (int i=0;i<neighbour_count_map.size();i++) {
            LinkedList<Spatial_Point> list_of_points_in_d=find_points_in_dist_d(dist_thresh,neighbour_count_map.get(i)._1(),neighbour_count_map);
            for(Spatial_Point point : list_of_points_in_d)
            {
                Spatial_Feature f=point.getFeature_type();
                int prev_value=neighbour_count_map.get(i)._2.get(f);
                System.out.println(prev_value);
                neighbour_count_map.get(i)._2.replace(f,prev_value+1);
            }
            for(Spatial_Point point : list_of_points_in_d)
            {
                double obj=1/neighbour_count_map.get(i)._2.get(point.getFeature_type());
                double prev_value=label_set.get(i)._2.get(point.getFeature_type());
                label_set.get(i)._2.replace(point.getFeature_type(),prev_value+obj);


            }
            for(Spatial_Point point : list_of_points_in_d)
            {
                if(label_set.get(i)._2.get(point.getFeature_type())>1)
                {
                    label_set.get(i)._2.replace(point.getFeature_type(),1D);
                }
            }
        }
//        label_set_rdd=sc.parallelizePairs(label_set);
        return label_set;

    }



    public static void main(String args[])
    {
        Random random=new Random();
        // adding point to the list
        LinkedList<Spatial_Point> points_lists=new LinkedList<Spatial_Point>();//list of all the points
        for(int i=0;i<20;i++)
        {
            int x=random.nextInt(100);
            int y=random.nextInt(100);
            points_lists.add(new Spatial_Point(x,y));
        }
        String[] arr={"A","B","C","D"};
        LinkedList<Spatial_Feature> spatial_features=new LinkedList<Spatial_Feature>();//set of all spatial features
        for(int i=0;i<arr.length;i++)
        {
            Spatial_Feature f=new Spatial_Feature(arr[i]);
            spatial_features.add(f);
        }

        for(int i=0;i<points_lists.size();i++)
        {
            int ind=random.nextInt(spatial_features.size());

            points_lists.get(i).setFeature_type(spatial_features.get(ind));
//            System.out.println(points_lists.get(i).getFeature_type().getFeature_name()+" "+points_lists.get(i).getX()+" "+points_lists.get(i).getY());
        }
        SparkSession spark= SparkSession.builder()
                .master("local")
                .appName("BDAProject")
                .getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        JavaRDD<Spatial_Point> points_rdd=sc.parallelize(points_lists);
        JavaRDD<Spatial_Feature> spatial_feature_rdd=sc.parallelize(spatial_features);
//        for(Spatial_Point person : points_rdd.collect()){
//            System.out.println(person.getFeature_type().getFeature_name()+" "+person.getX()+" "+person.getY());
//        }
//        for(Spatial_Feature f : spatial_feature_rdd.collect()){
//            System.out.println(f.getFeature_name());
//        }
        System.out.println("LABELS RDD");
        JavaPairRDD<Spatial_Point, HashMap<Spatial_Feature,Double>> label_set_rdd=sc.parallelizePairs(FractionComputation(points_rdd,spatial_feature_rdd,25));
        label_set_rdd.foreach(new VoidFunction<Tuple2<Spatial_Point, HashMap<Spatial_Feature, Double>>>() {
            @Override
            public void call(Tuple2<Spatial_Point, HashMap<Spatial_Feature, Double>> spatial_pointHashMapTuple2) throws Exception {
                Spatial_Point p=spatial_pointHashMapTuple2._1;
                HashMap<Spatial_Feature, Double> map=spatial_pointHashMapTuple2._2;
                System.out.println(p.getFeature_type().getFeature_name()+" "+p.getX()+" "+p.getY());
                for(Map.Entry m:map.entrySet())
                {
                    Spatial_Feature f= (Spatial_Feature) m.getKey();
                    System.out.println("   "+f.getFeature_name()+" "+m.getValue());
                }
            }
        });


    }
}
