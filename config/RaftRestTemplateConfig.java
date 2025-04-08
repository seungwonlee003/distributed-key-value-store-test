@Configuration
@RequiredArgsConstructor
class RaftRestTemplateConfig {
    private final RaftConfig raftConfig;

    @Bean
    public RestTemplate restTemplate() {
        int timeout = raftConfig.getHeartbeatIntervalMillis() / 2; // e.g., 25ms if interval is 50ms
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(timeout);
        factory.setReadTimeout(timeout);
        return new RestTemplate(factory);
    }
}
