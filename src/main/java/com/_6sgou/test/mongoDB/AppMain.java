package com._6sgou.test.mongoDB;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.ascending;
import static java.util.Arrays.asList;

@SpringBootApplication
public class AppMain {

    static Logger logger = (Logger) LoggerFactory.getLogger(AppMain.class);
    private MongoDatabase db;

    public static void main(String[] args) throws Exception{
        SpringApplication springApp = new SpringApplication(AppMain.class);
        springApp.setWebEnvironment(false); //<<<<<<<<<
        ConfigurableApplicationContext ctx = springApp.run(args);
        logger.info("=========== test app started ========");
        AppMain app = new AppMain();
        app.run();
        logger.info("=========== test app ended ========");
    }

    @Autowired
    private ApplicationContext context;

    public void run() throws Exception {
        MongoClient mongoClient = new MongoClient();
        db = mongoClient.getDatabase("test");

//        testInsert();
//        testFindAll();
//        testFindByBrought();
//        testFindByGrade();
//        testFindScoreGT();
//        testFindAndSort();
//        testGroupByBrought();
        testFilterAndGroup();
    }

    private void testInsert() throws ParseException {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
        db.getCollection("restaurants").insertOne(
                new Document("address",
                        new Document()
                                .append("street", "2 Avenue")
                                .append("zipcode", "10075")
                                .append("building", "1480")
                                .append("coord", asList(-73.9557413, 40.7720266)))
                        .append("borough", "Manhattan")
                        .append("cuisine", "Italian")
                        .append("grades", asList(
                                new Document()
                                        .append("date", format.parse("2014-10-01T00:00:00Z"))
                                        .append("grade", "A")
                                        .append("score", 11),
                                new Document()
                                        .append("date", format.parse("2014-01-16T00:00:00Z"))
                                        .append("grade", "B")
                                        .append("score", 17)))
                        .append("name", "Vella")
                        .append("restaurant_id", "41704620"));
    }

    private void testFindAll(){
        FindIterable<Document> iterable = db.getCollection("restaurants").find();
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
    }

    private void testFindByBrought(){
        // FindIterable<Document> iterable = db.getCollection("restaurants").find(new Document("borough", "Manhattan"));

        // OR
        FindIterable<Document> iterable = db.getCollection("restaurants").find(eq("borough", "Manhattan"));

        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
    }

    private void testFindByGrade(){
        FindIterable<Document> iterable = db.getCollection("restaurants").find(
                new Document("grades.grade", "B"));

        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
    }

    private void testFindScoreGT(){
        FindIterable<Document> iterable = db.getCollection("restaurants").find(
                new Document("grades.score", new Document("$gt", 30)));

        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
    }

    private void testFindAndSort(){
        FindIterable<Document> iterable = db.getCollection("restaurants").find().sort(ascending("borough", "address.zipcode"));
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
    }

    // group by account and count.
    private void testGroupByBrought(){

        System.out.printf("group by -- %s", new Document("_id", "$borough").append("count", new Document("$sum", 1)));
        AggregateIterable<Document> iterable = db.getCollection("restaurants").aggregate(asList(
                new Document("$group", new Document("_id", "$borough").append("count", new Document("$sum", 1)))));

        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        });
    }

    /**
     * Use the $match stage to filter documents. $match uses the MongoDB query syntax. The following pipeline uses $match
     * to query the restaurants collection for documents with borough equal to "Queens" and cuisine equal to Brazilian.
     * Then the $group stage groups the matching documents by the address.zipcode field and uses the $sum accumulator to
     * calculate the count. $group accesses fields by the field path, which is the field name prefixed by a dollar sign $.
     */
    private void testFilterAndGroup(){
        AggregateIterable<Document> iterable = db.getCollection("restaurants").aggregate(asList(
                new Document("$match", new Document("borough", "Queens").append("cuisine", "Brazilian")),
                new Document("$group", new Document("_id", "$address.zipcode").append("count", new Document("$sum", 1)))));

        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        });
    }

}
