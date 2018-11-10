package florian_stefan.spring_togglz_kafka_example;

import florian_stefan.spring_togglz_kafka_example.RedisStateRepository.Builder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RestController;
import org.togglz.core.manager.EnumBasedFeatureProvider;
import org.togglz.core.repository.StateRepository;
import org.togglz.core.spi.FeatureProvider;
import redis.clients.jedis.JedisPool;

@RestController
@SpringBootApplication
public class Application {

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Bean
  @SuppressWarnings("unchecked")
  public FeatureProvider featureProvider() {
    return new EnumBasedFeatureProvider(Features.class);
  }

  @Bean
  public StateRepository getStateRepository() {
    return new Builder().keyPrefix("togglz:").jedisPool(new JedisPool("redis")).build();
  }

}
