import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/*
 * Ratings: UserID::MovieID::Rating::Timestamp
 * Movies: MovieID::Title::Genres
 * Users: UserID::Gender::Age::Occupation::Zip-code
 * 
 * 
 * 
 */


object Mov {
  def main (args: Array[String]){
    val path = "/home/kati/foo/BDF/" // Path to files
    val conf = new SparkConf().setAppName("Movies").setMaster("local[2]")
    val sc = new SparkContext(conf)
    
    case class Rating (ruserid: Int, rmovieid: Int, rrating: Int, rtimestamp: Int)
    case class Movie (mmovieid: Int, mtitle: String, mgenre: String)
    case class User (uuserid: Int, ugender: String, uage: Int, uoccupation: Int, uzip: Int)
    
    
    
    val ratings = sc.textFile(path+"ratings.dat").map(_.split("::")).map(r => (r(1).toInt, Rating(r(0).toInt, r(1).toInt, r(2).toInt, r(3).toInt)))
    val movies = sc.textFile(path+"movies.dat").map(_.split("::")).map(r =>   (r(0).toInt,  Movie(r(0).toInt, r(1), r(2) )))
    val user = sc.textFile(path+"users.dat").map(_.split("::")).map(r =>   (r(0).toInt,  User(r(0).toInt, r(1), r(2).toInt, r(3).toInt, r(4).toInt )))
    
 
    println ("--------------------ratings: "+ratings.first)
    println ("--------------------movies: "+movies.first)
    println ("--------------------user: "+user.first)
    
    val ratingsWithMovies = ratings.join(movies)
    println ("--------------------ratingsWithMovies: "+ratingsWithMovies.first)
    //ratingsWithMovies: (2828,(Rating(16,2828,1,978174535),Movie(2828,Dudley Do-Right (1999),Children's|Comedy)))
      
    
    // Emil ratings and movies ratingsAndMovies.map {x => val (movieid,(rating,movie)) = x}
    //sitten pääsee eroon näistä alaviivoista ja voi käyttää noita nimiä
    //val ratingsWithMoviesByUser = ratingsWithMovies.map(x => )
    
    val moviesWithRatings = movies.join(ratings)
    println ("--------------------moviesWithRatings: "+moviesWithRatings.first)
    //moviesWithRatings: (2828,(Movie(2828,Dudley Do-Right (1999),Children's|Comedy),Rating(16,2828,1,978174535)))
    
    
    /*
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
 
    case class Register (d: java.util.Date, uuid: String, cust_id: String, lat: Float, lng: Float)
    case class Click (d: java.util.Date, uuid: String, landing_page: Int)
 
    val reg = sc.textFile("reg.tsv").map(_.split("\t")).map(
      r => (r(1), Register(format.parse(r(0)), r(1), r(2), r(3).toFloat, r(4).toFloat))
      )
 
    val clk = sc.textFile("clk.tsv").map(_.split("\t")).map(
      c => (c(1), Click(format.parse(c(0)), c(1), c(2).trim.toInt))
      )
 
    reg.join(clk).take(2)
*/

  }

}