//import sttp.client3.quick._
//
//object TestCurl extends App {
//
//  val username = "A5IG2N4JNMSCX32A"
//  val password = "8VKJZLmy8asuXrN/2sEs5JThuKUcu95DUlhKcxna9SRYVlXeu4+UCZ5DYAdqvICJ"
//
//
//  val response = quickRequest
//    .auth.basic(username, password)
//    .get(uri"https://psrc-xm8wx.eu-central-1.aws.confluent.cloud/subjects/test_2-value/versions/latest")
//    .send(backend)
//
//  println(response.code)
//  // prints: 200
//
//  println(response.body)
//
//
//}
