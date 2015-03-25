/*Kati Kyllönen
 * 011539913
 * Big Data Framework
 * Exercise set 2
 * Exercise 2 Movies
 * 
 * Ratings:
 * UserID::MovieID::Rating::Timestamp
 * 
 * Movies:
 * MovieID::Title::Genres
 * 
 * Users:
 * UserID::Gender::Age::Occupation::Zip-code
 * 
 * Target:
 * RDD[(userID, gender, age, occupation, Set(movie, genre, rating))]
 * Be sure that you choose correct data types, e.g., IDs are Integers. 
 * 
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Movies {
  
  def main (args: Array[String]){
    val path = "/home/kati/foo/BDF/" // Path to files
    val conf = new SparkConf().setAppName("Movies").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // read the files as RDDs and cache them
    val movies = sc.textFile(path+"movies.dat", 2).cache()
    val users = sc.textFile(path+"users.dat", 2).cache()
    val ratings = sc.textFile(path+"ratings.dat", 2).cache()
    
    //val movieMap = movies.map { x => x.split("::")}
    //val usersMap = users.map { x => x.split("::") }
    //val ratingsMap = ratings.map { x => x.split("::") }
    
    val splittedUsersMap = users.map(line => line.split("::")).map(x => (x(0).toInt, (x(1), x(2).toInt, x(3).toInt)))
     println ("........-----splittedUsersMap" +splittedUsersMap.first() )
     //userId -> (gender, age, occupation) 
    
    val splittedMoviesMap = movies.map(line => line.split("::")).map(x => (x(0).toInt, (x(1), x(2)))) 
     println ("........-----splittedMoviesMap" +splittedMoviesMap.first() )
    // //movieId -> (Title, Genre)
     
    val splittedRatingsMap = ratings.map(line => line.split("::")).map(x => (x(1).toInt, (x(0).toInt, x(2).toInt, x(3).toInt))) 
     println ("........-----splittedRatingsMap" +splittedRatingsMap.first() )
     //movieId -> (UserId, Rating, Timestamp)
   
     val splittedRatingsMapByUser = ratings.map(line => line.split("::")).map(x => (x(0).toInt, (x(1).toInt, x(2).toInt, x(3).toInt))) 
     println ("........-----splittedRatingsMap" +splittedRatingsMap.first() )
     //userId -> (movieId, Rating, Timestamp)
    
     
     val moviesAndRatings = splittedMoviesMap.join(splittedRatingsMap)
     println (":::::::::::::::::::::::::"+moviesAndRatings.values.first())
     //Title, Genre, userID, Rating, Timestamp
     //((Bonnie and Clyde (1967),Crime|Drama),(2,3,978298813))
     
     val moviesAndRatingsByUser = moviesAndRatings.values.map(x => (x._2._1, (x._1._1, (x._1._2, x._2._2))))
     println ("******************"+moviesAndRatingsByUser.first())
     //userId -> (Title, Genre, Rating)
     //(2,(Bonnie and Clyde (1967),Crime|Drama,3))
     
     val res = splittedUsersMap.join(moviesAndRatingsByUser).groupByKey
     println ("-.-.-.-.-.-.-.-.-.-.-."+res.first())
     //userID,(Gender, Age, Occupation)(Title, Genre, Rating)...
     //(2,CompactBuffer(((M,56,16),(Bonnie and Clyde (1967),(Crime|Drama,3))), ((M,56,16),(Maverick (1994),(Action|Comedy|Western,4))),... 
     
    val res1 = res.map(x => (x._1, x._2.toSet ))
    println(""""""""""""""""""+res1.first())
    //userID, Set(Gender, Age, Occupation)( Title, Genre,Rating)
    
    
     def numberOfWatchedMovies {
      val adultAge = 18
      //val res.filter(x => (x.1_.2 >= adultAge))
        
    }
   
    
         
         
     
      
  }

}