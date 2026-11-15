package com.kbt.amumal.domain.search.service;

import com.kbt.amumal.domain.post.dto.PostResDTO;
import com.kbt.amumal.domain.post.repository.PostRepository;
import com.kbt.amumal.domain.search.document.PostDocument;
import com.kbt.amumal.domain.search.dto.PostSearchResDTO;
import jakarta.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.transaction.support.TransactionTemplate;
import com.kbt.amumal.domain.search.repository.SearchIndexDirtyRepository;
import java.time.LocalDateTime;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.util.Locale;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

// LIKE 서비스와 ES 서비스의 단일 사용자 검색 지연을 규모별로 반복 측정한다.
// 반드시 전용 searchBenchmark 태스크로 amumal-dev의 분리된 DB/인덱스에서 실행한다.
// search.enabled은 integration 프로필 기본값이 false라 이 테스트에서만 true로 켠다
@Slf4j
@Tag("search-benchmark")
@ActiveProfiles("integration")
@SpringBootTest(properties = {"search.enabled=true", "search.sync.fixed-delay-ms=86400000", "search.sync.initial-delay-ms=86400000",
        "logging.level.org.hibernate.stat=OFF", "logging.level.org.hibernate.engine.internal.StatisticalLoggingSessionEventListener=OFF"})
class PostSearchPerformanceBenchmarkTest {

    private static final int KEYWORD_HIT_COUNT = 15;
    private static final int PAGE_SIZE = 20;
    // 형태소 분석/오타 허용 이슈와 무관하게 순수 색인 방식 성능만 비교하기 위한, 다른 글자와 안 섞이는 전용 키워드
    private static final String KEYWORD = "BENCHMARKTOKEN";


    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Autowired
    private PostRepository postRepository;

    @Autowired
    private PostSearchService postSearchService;

    @Autowired
    private PostSearchIndexGateway indexGateway;

    @Autowired
    private ElasticsearchOperations elasticsearchOperations;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private PostLikeSearchService postLikeSearchService;

    @Autowired
    private SearchDirtyService searchDirtyService;

    @Autowired
    private SearchIndexSyncService searchIndexSyncService;

    @Autowired
    private SearchIndexDirtyRepository dirtyRepository;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Value("${search.sync.batch-size:500}")
    private int syncBatchSize;

    private static final List<String> samples = new ArrayList<>(List.of("posts,iteration,engine,elapsed_ms,sql_statements"));
    private static final List<String> storage = new ArrayList<>(List.of("posts,mysql_load_and_dirty_ms,incremental_sync_ms,mysql_table_bytes,es_primary_store_bytes"));
    private List<Integer> keywordPostIds;
    private Statistics statistics;

    @Value("${search.index-name}")
    private String indexName;

    @BeforeEach
    void setUp() {
        assertThat(indexName).isEqualTo("posts-search-benchmark");
        assertThat(jdbc.queryForObject("SELECT DATABASE()", String.class)).isEqualTo("search_benchmark");
        assertThat(elasticsearchOperations.getIndexCoordinatesFor(PostDocument.class).getIndexName())
                .isEqualTo("posts-search-benchmark");
        // 중단된 이전 실행의 ES 문서는 DB의 create-drop으로 제거되지 않는다.
        // 검증된 벤치마크 전용 인덱스만 시작 전에 초기화한다.
        var index = elasticsearchOperations.indexOps(PostDocument.class);
        if (index.exists()) {
            index.delete();
        }
        statistics = entityManagerFactory.unwrap(SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true);
    }

    @AfterEach
    void tearDown() {
        jdbc.update("DELETE FROM search_index_dirty");
        jdbc.update("DELETE FROM posts");
        // 건별로 지우는 대신 인덱스를 통째로 지운다 (다음 테스트의 ensureIndex가 재생성)
        elasticsearchOperations.indexOps(PostDocument.class).delete();
    }

    @AfterAll
    static void printSummary() throws IOException {
        Path directory = Path.of("build/reports/search-benchmark");
        Files.createDirectories(directory);
        Files.write(directory.resolve("samples.csv"), samples);
        Files.write(directory.resolve("storage.csv"), storage);
    }

    @ParameterizedTest(name = "post {0}건일 때 LIKE vs ES 비교")
    @ValueSource(ints = {100, 500, 1_000, 3_000, 10_000, 30_000, 100_000, 300_000, 1_000_000})
    @DisplayName("키워드 검색 시 데이터 규모별로 LIKE 풀스캔과 ES 검색의 쿼리 개수·소요시간을 비교한다")
    void compareLikeAndElasticsearch(int postCount) throws Exception {
        seedData(postCount);

        // 각 규모마다 두 경로를 워밍업하고 측정 순서를 번갈아 실행한다.
        for (int i = 0; i < 5; i++) {
            assertThat(searchByLike()).containsExactlyInAnyOrderElementsOf(keywordPostIds);
            assertThat(searchByElasticsearch()).containsExactlyInAnyOrderElementsOf(keywordPostIds);
        }
        List<Double> likeTimes = new ArrayList<>();
        List<Double> esTimes = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            if (i % 2 == 0) {
                recordSample(postCount, i, "LIKE", this::searchByLike, likeTimes);
                recordSample(postCount, i, "ES", this::searchByElasticsearch, esTimes);
            } else {
                recordSample(postCount, i, "ES", this::searchByElasticsearch, esTimes);
                recordSample(postCount, i, "LIKE", this::searchByLike, likeTimes);
            }
        }
        likeTimes.sort(Double::compareTo);
        esTimes.sort(Double::compareTo);
        log.info("RESULT posts={} LIKE p50={}ms p95={}ms ES p50={}ms p95={}ms",
                postCount, likeTimes.get(14), likeTimes.get(28), esTimes.get(14), esTimes.get(28));
    }

    private void recordSample(int count, int iteration, String engine, Supplier<Set<Integer>> action,
                              List<Double> times) {
        statistics.clear();
        long start = System.nanoTime();
        Set<Integer> ids = action.get();
        double elapsed = (System.nanoTime() - start) / 1_000_000.0;
        long sql = statistics.getPrepareStatementCount();
        assertThat(ids).containsExactlyInAnyOrderElementsOf(keywordPostIds);
        times.add(elapsed);
        samples.add(String.format(Locale.ROOT, "%d,%d,%s,%.3f,%d", count, iteration, engine, elapsed, sql));
    }

    private void seedData(int postCount) throws Exception {
        log.info("더미데이터 적재 시작: {}건", postCount);
        indexGateway.ensureIndex();
        keywordPostIds = new ArrayList<>();
        LocalDateTime created = LocalDateTime.of(2026, 1, 1, 0, 0);
        long dbNanos = 0;
        // IDENTITY 기반 JPA saveAll은 JDBC 배치 INSERT가 되지 않아 준비 단계는 JDBC를 사용한다.
        // 검색 결과가 처음/끝에 몰리지 않도록 15개 문서를 전체 범위에 분산한다.
        Set<Integer> matches = IntStream.range(0, Math.min(KEYWORD_HIT_COUNT, postCount))
                .map(i -> 1 + (int) ((long) i * postCount / Math.min(KEYWORD_HIT_COUNT, postCount)))
                .boxed().collect(Collectors.toSet());
        for (int offset = 0; offset < postCount; offset += 1000) {
            List<Object[]> rows = new ArrayList<>();
            for (int id = offset + 1; id <= Math.min(offset + 1000, postCount); id++) {
                String title = "일본 여행 후기 " + id;
                String content = "이번 여행 정말 좋았어요 별다른 특이사항은 없었습니다 " + id
                        + (matches.contains(id) ? " " + KEYWORD : "");
                LocalDateTime date = created.plusSeconds(id);
                rows.add(new Object[]{id, title, content, 1, 0, 0, 0, date, date});
                if (matches.contains(id)) keywordPostIds.add(id);
            }
            long start = System.nanoTime();
            // 게시글 저장과 실제 markDirty를 같은 트랜잭션으로 묶는다.
            transactionTemplate.executeWithoutResult(status -> {
                jdbc.batchUpdate("INSERT INTO posts (post_id,title,content,user_id,view_count,like_count,comment_count,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)", rows);
                rows.forEach(row -> searchDirtyService.markDirty((Integer) row[0]));
            });
            dbNanos += System.nanoTime() - start;
            if ((offset + 1000) % 100000 == 0) log.info("적재 진행: {} / {}", offset + 1000, postCount);
        }
        assertThat(dirtyRepository.count()).isEqualTo(postCount);
        long syncStart = System.nanoTime();
        log.info("[벤치마크] 증분 동기화 시작 - 대상 {}건, 배치 {}건", postCount, syncBatchSize);
        int attempted = 0;
        // 스케줄러의 대기 시간만 생략하고, 실제 선점·DB 조회·문서 생성·ES 반영·dirty 삭제를 수행한다.
        while (attempted < postCount) {
            int processed = searchIndexSyncService.synchronizeNextBatch(syncBatchSize);
            assertThat(processed).as("처리 가능한 dirty 대상이 있어야 합니다. 실패/재시도 대기 여부를 확인하세요.")
                    .isPositive();
            attempted += processed;
            if (attempted % 100000 == 0 || attempted >= postCount) {
                log.info("[벤치마크] 증분 동기화 진행 - 처리 시도 {} / {}건", attempted, postCount);
            }
        }
        assertThat(dirtyRepository.count())
                .as("동기화 실패 또는 미완료 dirty가 남아 있으면 검색 성능 측정을 시작하지 않습니다.")
                .isZero();
        elasticsearchOperations.indexOps(PostDocument.class).refresh();
        long syncNanos = System.nanoTime() - syncStart;
        assertThat(postRepository.count()).isEqualTo(postCount);
        assertThat(elasticsearchOperations.count(org.springframework.data.elasticsearch.core.query.Query.findAll(), PostDocument.class))
                .isEqualTo(postCount);
        log.info("[벤치마크] 증분 동기화 완료 - MySQL {}건, ES {}건, dirty 0건, refresh 포함 {}ms",
                postCount, postCount, syncNanos / 1_000_000);
        // MySQL은 ANALYZE 후의 추정 할당 크기, ES는 primary store 크기를 기록한다.
        jdbc.execute("ANALYZE TABLE posts");
        Long mysqlBytes = jdbc.queryForObject("SELECT data_length + index_length FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'posts'", Long.class);
        try (var http = java.net.http.HttpClient.newHttpClient()) {
            var response = http.send(java.net.http.HttpRequest.newBuilder(
                    java.net.URI.create("http://127.0.0.1:9200/posts-search-benchmark/_stats/store")).GET().build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            var json = new tools.jackson.databind.ObjectMapper().readTree(response.body());
            long esBytes = json.path("_all").path("primaries").path("store").path("size_in_bytes").asLong();
            storage.add(postCount + "," + dbNanos / 1_000_000 + "," + syncNanos / 1_000_000 + "," + mysqlBytes + "," + esBytes);
        }
        log.info("적재 완료: {}건 MySQL+dirty 등록={}ms 증분 동기화(최종 refresh 포함)={}ms", postCount, dbNanos / 1_000_000, syncNanos / 1_000_000);
    }

    private Set<Integer> searchByLike() {
        return postLikeSearchService.search(KEYWORD, PAGE_SIZE).stream()
                .map(PostResDTO.PostListItem::postId)
                .collect(Collectors.toSet());
    }

    private Set<Integer> searchByElasticsearch() {
        PostSearchResDTO.response response = postSearchService.search(KEYWORD, "latest", null, PAGE_SIZE);
        return response.posts().stream()
                .map(PostResDTO.PostListItem::postId)
                .collect(Collectors.toSet());
    }

}
