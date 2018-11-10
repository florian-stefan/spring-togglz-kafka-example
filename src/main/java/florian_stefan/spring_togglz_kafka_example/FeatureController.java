package florian_stefan.spring_togglz_kafka_example;

import static org.springframework.http.ResponseEntity.notFound;

import java.util.Optional;
import java.util.function.Predicate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.togglz.core.Feature;
import org.togglz.core.manager.FeatureManager;
import org.togglz.core.repository.FeatureState;

@RestController
public class FeatureController {

  private final FeatureManager featureManager;

  public FeatureController(FeatureManager featureManager) {
    this.featureManager = featureManager;
  }

  @GetMapping("/features/{name}")
  public ResponseEntity<FeatureState> getFeature(@PathVariable String name) {
    return findFeature(name).map(featureManager::getFeatureState).map(ResponseEntity::ok).orElse(notFound().build());
  }

  @GetMapping("/features/{name}/toggle")
  public void toggleFeature(@PathVariable String name) {
    findFeature(name).ifPresent(this::toggleFeature);
  }

  private Optional<Feature> findFeature(String name) {
    return featureManager.getFeatures().stream().filter(hasName(name)).findFirst();
  }

  private Predicate<Feature> hasName(String name) {
    return feature -> name.toUpperCase().equals(feature.name());
  }

  private void toggleFeature(Feature feature) {
    boolean active = featureManager.isActive(feature);

    featureManager.setFeatureState(new FeatureState(feature, !active));
  }

}
