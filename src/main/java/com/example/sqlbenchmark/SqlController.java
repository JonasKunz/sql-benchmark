package com.example.sqlbenchmark;

import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@RestController("/")
public class SqlController {

    private final List<Double> durations = new ArrayList<>();

    private final JdbcTemplate template;

    private final RestClient rc;

    private final ConfigurableApplicationContext ctx;

    public SqlController(JdbcTemplate template, ConfigurableApplicationContext ctx) {
        this.template = template;
        this.rc = RestClient.create();
        this.ctx = ctx;
        initDb(template);
        new Thread(this::runBenchmark).start();
    }

    private void runBenchmark() {
        try {

            Runnable request = () -> rc.get().uri("http://localhost:8080/queries").retrieve().body(String.class);

            Thread.sleep(1000);
            System.out.println("Running warm up for 15 seconds..");
            repeatForDuration(Duration.ofSeconds(30), request);
            printDurations();
            resetDurations();

            System.out.println("Running measurement for 60 seconds..");
            repeatForDuration(Duration.ofSeconds(120), request);
            printDurations();

        }catch (Exception e) {
            throw new RuntimeException(e);
        }
        ctx.close();
    }

    private void repeatForDuration(Duration d, Runnable r) {
        long start = System.nanoTime();
        while((System.nanoTime() - start) < d.toNanos()) {
            r.run();
        }
    }

    private void initDb(JdbcTemplate template) {
        template.update("CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) NOT NULL)");

        for(int i=1; i<=100; i++) {
            template.update("""
                                INSERT INTO customers (id, name, email)
                                VALUES (?,?,?)
                                """, i, "username"+i, "user."+i+"@mail.com");
        }
    }

    private final AtomicInteger uniqueVal = new AtomicInteger();

    @GetMapping("queries")
    public Object runQueries() {
        long start = System.nanoTime();
        int sum = 0;
        int unique = uniqueVal.incrementAndGet();
        String whereLengthening = generateLongWhere(300, unique);
        for (int i=0; i<1000; i++) {
            String queryString = "SELECT COUNT(*) AS cnt FROM customers WHERE name LIKE ?" + whereLengthening;
            sum += template.queryForObject(queryString, (rs, rowNum) -> {
                return rs.getInt(rs.findColumn("cnt") );
            }, "%42%");
        }
        long dur = System.nanoTime()- start;
        synchronized (this) {
            durations.add(dur/1_000_000.0);
        }
        return ""+sum;
    }

    private String generateLongWhere(int count, int dummyVal) {
        StringBuilder result = new StringBuilder();
        for(int i=0; i<count; i++) {
            result.append(" AND "+dummyVal+" = "+dummyVal);
        }
        return result.toString();
    }

    public synchronized void resetDurations() {
        durations.clear();
    }

    public synchronized void printDurations() {

        double[] durationsAsDoubles = durations.stream()
                .mapToDouble(Double::doubleValue)
                .toArray();
        int cnt = durations.size();
        System.out.println("Num Executions: "+ cnt);
        if(cnt > 0) {
            double avg = durations.stream().mapToDouble(i -> i).average().getAsDouble();
            double stdDev = new StandardDeviation().evaluate(durationsAsDoubles);
            double median = new Percentile().evaluate(durationsAsDoubles, 50.0);
            double p90 = new Percentile().evaluate(durationsAsDoubles, 90.0);
            double p95 = new Percentile().evaluate(durationsAsDoubles, 95.0);
            double p99 = new Percentile().evaluate(durationsAsDoubles, 99.0);

            System.out.println("Avg: "+avg+", std-dev: "+stdDev);
            System.out.println("p50: "+median);
            System.out.println("p90: "+p90);
            System.out.println("p95: "+p95);
            System.out.println("p99: "+p99);
        }
    }
}
