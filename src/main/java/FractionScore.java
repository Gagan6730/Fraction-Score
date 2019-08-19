import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import shapeless.Tuple;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

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
    public static LinkedList<Spatial_Point> find_points_in_dist_d(int d,Spatial_Point point,JavaRDD<Spatial_Point> points_rdd)
    {
        LinkedList<Spatial_Point> list_of_points_in_d=new LinkedList<>();
        for (Spatial_Point p:points_rdd.collect())
        {
            if(p.equals(point))
            {
                continue;
            }
            else
            {
                if(calca_dist(p,point)>=d)
                {
                    list_of_points_in_d.add(p);
                }
            }
        }
        return list_of_points_in_d;
    }
    public static void FractionComputation(JavaRDD<Spatial_Point> points_rdd,JavaRDD<Spatial_Feature> spatial_feature_rdd, int dist_thresh,JavaSparkContext sc)
    {
        List<Tuple2<Spatial_Point, List<Tuple2<Spatial_Feature,Integer>>>> neighbour_count_map=new LinkedList<>();
        List<Tuple2<Spatial_Point, List<Tuple2<Spatial_Feature,Double>>>> label_set=new LinkedList<>();
        JavaPairRDD<Spatial_Point, HashMap<Spatial_Feature,Double>> label_set_rdd;
        for (Spatial_Point p:points_rdd.collect()) {
            List<Tuple2<Spatial_Feature,Integer>> map_neigh=new LinkedList<>();
            List<Tuple2<Spatial_Feature,Double>> map_label=new LinkedList<>();
            for (Spatial_Feature f:spatial_feature_rdd.collect())
            {
                map_neigh.add(new Tuple2<>(f,0));
                map_label.add(new Tuple2<>(f,0D));

            }
            neighbour_count_map.add(new Tuple2<>(p,map_neigh));
            label_set.add(new Tuple2<>(p,map_label));
        }
        points_rdd.count()
        for (int i=0;i<neighbour_count_map.size();i++) {
            LinkedList<Spatial_Point> list_of_points_in_d=find_points_in_dist_d(dist_thresh,p,points_rdd);
            for(Spatial_Point point : list_of_points_in_d)
            {
                Spatial_Feature f=point.getFeature_type();
                int prev_value=neighbour_count_map.get(i)._2().
//                        get(p).get(f);
                neighbour_count_map.get(i)._2().get()
//                        .replace(f,prev_value,prev_value+1);
            }
            for(Spatial_Point point : list_of_points_in_d)
            {
                double obj=1/neighbour_count_map.get(p).get(point.getFeature_type());
                double prev_value=label_set.get(p).get(point.getFeature_type());
                label_set.get(p).replace(point.getFeature_type(),prev_value+obj);


            }
            for(Spatial_Point point : list_of_points_in_d)
            {
                if(label_set.get(p).get(point.getFeature_type())>1)
                {
                    label_set.get(p).replace(point.getFeature_type(),1D);
                }
            }
        }
//        label_set_rdd=sc.para

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


    }
}
