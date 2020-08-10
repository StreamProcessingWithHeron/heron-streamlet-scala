import java.util.concurrent.ThreadLocalRandom

import org.apache.heron.api.utils.Utils

import org.apache.heron.streamlet.Config
import org.apache.heron.streamlet.scala.{Builder, Runner}

object ExclamationTopology {
  def main(args: Array[String]): Unit = {
    val words = 
      Array("nathan", "mike", "jackson", "golda", "bertels")
    val builder = Builder.newBuilder 

    val s1 = builder.newSource(() => {
      Utils.sleep(500)
      words(ThreadLocalRandom.current().nextInt(words.length))
    }).setName("s1") 
    val s2 = builder.newSource(() => {
      Utils.sleep(500)
      val word2 = 
        words(ThreadLocalRandom.current().nextInt(words.length))
      val word3 = 
        words(ThreadLocalRandom.current().nextInt(words.length))
      Array(word2, word3)
    }).setName("s2") 

    val s3 = s1.map(x => x+" !!!") 
    val s4 = s2.map(x => x(0)+" & "+x(1)+" !!!") 
    s3.union(s4).map(x => x+" !!!").log() 

    val config = Config.newBuilder().setNumContainers(2)
                       .build() 

    new Runner().run("my-scala-streamlet", config, builder) 
  }
}