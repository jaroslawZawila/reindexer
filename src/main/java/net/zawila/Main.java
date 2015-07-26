package net.zawila;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.util.Random;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(final String[] args) {

//        ClientManager manager = new ClientManager("test", "type", "localhost");
//        LOG.info("Populate index");
//        Random random = new Random();
//        DateTime time = new DateTime();
//
//        for(int day =0; day<10 ; day++) {
//            for(int i=0; i<100; i++){
//                int number = random.nextInt(100000);
//                manager.getClient().prepareIndex("test", "type").setSource("type", "standard", "filename", number + "-test.pdf", "timestamp", time.minusDays(day)).get();
//            }
//        }

        new Main().run();

    }

    public void run() {
        LOG.info("Started re-indexing process");

        ClientManager manager = new ClientManager("test", "type", "localhost");

        DateTime currentTime = new DateTime().withTime(0,0,0,0).plusDays(1);

        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMdd");
        DateTime endDate = fmt.parseDateTime("20150725");

        long numberOfDocumentInPeriod = manager.getDocumentNumberInPeriod(currentTime.minusDays(1), currentTime);

        LOG.info("Founded {} items.", numberOfDocumentInPeriod);

        while (true) {
            DateTime beforeDate = currentTime.minusDays(1);

            manager.reindex(beforeDate, currentTime, "testnew");

            currentTime = beforeDate;
            if(beforeDate.equals(endDate)){
                break;
            }
        }

    }

  }