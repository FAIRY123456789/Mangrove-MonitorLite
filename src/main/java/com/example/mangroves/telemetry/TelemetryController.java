package com.example.mangroves.telemetry;

import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class TelemetryController {
    private final TelemetryRepository repository;

    public TelemetryController(TelemetryRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/telemetry")
    public List<TelemetryMessage> telemetry(
            @RequestParam String deviceId,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to) {
        return repository.findTelemetry(deviceId, from, to);
    }

    @GetMapping("/anomalies")
    public List<AnomalyResult> anomalies(
            @RequestParam String deviceId,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(required = false) String type) {
        return repository.findAnomalies(deviceId, from, to, type);
    }

    @GetMapping("/telemetry/{id}")
    public ResponseEntity<Map<String, Object>> telemetryWithAnomalies(@PathVariable long id) {
        return repository.findTelemetryById(id)
                .map(telemetry -> {
                    Map<String, Object> response = new LinkedHashMap<>();
                    response.put("telemetry", telemetry);
                    response.put("anomalies", repository.findAnomaliesByTelemetryId(id));
                    return ResponseEntity.ok(response);
                })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
