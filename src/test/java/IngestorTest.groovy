import in.oogway.plumbox.launcher.ingestor.Ingestor
import spock.lang.Specification

class IngestorTest extends Specification{
   def "execute"() {
       setup:
       Ingestor.metaClass.execute = {true}
       System.setProperty("redis_server_address", "localhost")
       and:
       String ingestorID = "i01"
       def handler = new Ingestor(ingestorID)
       when:
       def res = handler.execute()
       then:
       res == true
    }
}