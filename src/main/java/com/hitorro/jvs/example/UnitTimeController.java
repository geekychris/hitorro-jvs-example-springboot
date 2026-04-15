/*
 * Copyright (c) 2006-2025 Chris Collins
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.hitorro.jvs.example;

import com.hitorro.unittime.UnitTimeContext;
import com.hitorro.unittime.UnitTimeResult;
import com.hitorro.util.core.CPUInfo;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/unittime")
@CrossOrigin(origins = "*")
public class UnitTimeController {

    private final UnitTimeContext context = new UnitTimeContext();
    private double detectedGhz = 0;

    private synchronized double getCpuGhz() {
        if (detectedGhz == 0) {
            try {
                detectedGhz = CPUInfo.getCPUClockSpeedGHz();
            } catch (Exception e) {
                detectedGhz = 4.0;
            }
            if (detectedGhz <= 0) detectedGhz = 4.0;
        }
        return detectedGhz;
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("cpuGhz", getCpuGhz());
        status.put("jvmVersion", System.getProperty("java.version"));
        status.put("osName", System.getProperty("os.name"));
        status.put("osArch", System.getProperty("os.arch"));
        status.put("timerCount", context.getTimerCount());
        return ResponseEntity.ok(status);
    }

    @GetMapping("/categories")
    public ResponseEntity<List<String>> getCategories() {
        return ResponseEntity.ok(context.getCategories());
    }

    @GetMapping("/run")
    public synchronized ResponseEntity<List<Map<String, Object>>> runBenchmarks(
            @RequestParam(required = false) String filter,
            @RequestParam(required = false, defaultValue = "0") double ghz) {

        double cpuGhz = ghz > 0 ? ghz : getCpuGhz();

        List<UnitTimeResult> results;
        if (filter != null && !filter.isBlank()) {
            results = context.runFiltered(filter, cpuGhz);
        } else {
            results = context.runAll(cpuGhz);
        }

        List<Map<String, Object>> response = new ArrayList<>();
        for (UnitTimeResult r : results) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("category", r.category);
            row.put("subCategory", r.subCategory);
            row.put("description", r.description);
            row.put("msPerUnit", r.milliSecondsPerUnit);
            row.put("nsPerUnit", r.nsPerUnit);
            row.put("instructionCycles", r.instructionCycles);
            row.put("units", r.units);
            if (r.error != null) {
                row.put("error", r.error.getMessage());
            }
            response.add(row);
        }
        return ResponseEntity.ok(response);
    }
}
