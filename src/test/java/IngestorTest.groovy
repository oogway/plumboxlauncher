import in.oogway.library.ingestor.Ingestor
import spock.lang.Specification

import javax.ws.rs.core.Response

class IngestorTest extends Specification{
    def "filterData"() {
        given:
        String ingestorFileName = "i01";
        when:
        Ingestor ingestorObj = new Ingestor();
        Response response = ingestorObj.loadContent(ingestorFileName);
        then:
        response.getStatusInfo() == Response.Status.CREATED;
    }
}
