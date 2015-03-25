import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Movies {
  
  def main (args: Array[String]){
    val path = "/home/kati/foo/BDF/" // Path to files
    val conf = new SparkConf().setAppName("Movies").setMaster("local")
    val sc = new SparkContext(conf)
    val movies = sc.textFile(path+"movies.dat", 2).cache()
    val users = sc.textFile(path+"users.dat", 2).cache()
    val ratings = sc.textFile(path+"ratings.dat", 2).cache()
    
    
    val movieMap = movies.map { x => x.split("::")}
    val usersMap = users.map { x => x.split("::") }
    val ratingsMap = ratings.map { x => x.split("::") }
    
    val moviePairs = movies.map(x => (x.split("::")(0), x))
    val userPairs = users.map(x => (x.split("::")(0), x))
    val ratingsPairs = ratings.map(x => (x.split("::")(0), x))
    
    println ("-------------user: "+userPairs.first())
    println ("-------------movie: "+moviePairs.first())
    println ("-------------rating: "+ratingsPairs.first())
    
    val usersAndRatings = userPairs.++(ratingsPairs).groupByKey()
    val usersAndRatingsuu = usersAndRatings.map(x => x.swap);
    
    println ("----------------------userAndRating: "+usersAndRatings.first())
    println ("----------------------userAndRatinguu: "+usersAndRatingsuu.first())
  
    
        
    
    
    
      
      
  }

}