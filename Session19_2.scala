package sql

import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf

case class Schema ( firstname:String,
                    lastname:String,
                    sports:String,
                    medal_type:String,
                    age:Int,
                    year:Int,
                    country:String )   //Will be applied to Dataframe as scheme

object Session19_2 extends App{

  val spark = SparkSession.builder().master("local")
                                    .appName("S19_A2")
                                    .config("spark.sql.warehouse.dir", "/home/sam/work")
                                    .getOrCreate()
  import spark.implicits._

  val sport_df = spark.read.format("com.databricks.spark.csv")
                      .option("header", "true")
                      .load("/home/sam/work/input/Sports_data.txt")

                      .map{ case Row(a:String, b:String, c:String, d:String, e:String, f:String, g:String)
                              => Schema(a, b, c, d, e.toInt, f.toInt, g)}

  // 1. Change firstname, lastname columns into  Mr.first_two_letters_of_firstname<space>lastname

  def namer(first: String, last:String): String = "Mr."+ first.substring(0,2) +" "+ last

  val nameUDF = udf(namer _) //creating UDF with function namer

  val sportName_df = sport_df.select( nameUDF(sport_df("firstname"), sport_df("lastname")) as "fullname",   //UDF column
                                      sport_df("sports"),
                                      sport_df("medal_type"),
                                      sport_df("age"),
                                      sport_df("year"),
                                      sport_df("country"))
  sportName_df.show(5)










  /** 2.  Add a new column called ranking using udfs on dataframe, where :
    *     gold medalist, with age >= 32 are ranked as pro else amateur
    *     silver medalist, with age >= 32 are ranked as expert else rookie
    */

  def rank(medal_type: String, age:Int): String = medal_type match {
    case "gold" => age match{
                              case x if x > 31 => "pro"               // if medal is gold & age > 31 it's a pro
                              case x if x < 32 => "amateur"           // if medal is gold & age < 32 it's an amateur
                              case _ => "NA"
    }

    case "silver" => age match{
                              case x if x > 31 => "expert"            // if medal is silver & age > 31 it's an expert
                              case x if x < 32 => "rookie"            // if medal is silver & age < 32 it's a rookie
                              case _ => "NA"
    }
    case _ => "NA"
  }

  val rankUDF = udf(rank _)

  val sportRank_df = sportName_df.select( sportName_df("*"),       //Selecting all columns + Adding new column with UDF
                                          rankUDF(sportName_df("medal_type"), sportName_df("age")) as "ranking")
  sportRank_df.show(5)

}
