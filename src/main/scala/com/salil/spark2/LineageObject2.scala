package com.salil.spark2

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

case class LineageObject4(@JsonProperty applicationId: String, @JsonProperty name: String){

  @JsonProperty
  var newProperty = "new"

  def myMethod() = {
    val lineageObject = new LineageObject4("app","name")
    lineageObject.newProperty = "myymy"
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    print("JSON : " + mapper.writeValueAsString(lineageObject))
  }

}

class LineageObject2{
  var name:String = ""
  var id:Int = 0
}

import org.json4s.jackson.Serialization
import org.json4s._
import scala.util.Try

object JSONUtil {

  implicit val formats = DefaultFormats + FieldSerializer[LineageObject2]()


  def toJSON(objectToWrite: AnyRef): String = Serialization.write(objectToWrite)

  def fromJSONOption[T](jsonString: String)(implicit mf: Manifest[T]): Option[T] = Try(Serialization.read(jsonString)).toOption

}

