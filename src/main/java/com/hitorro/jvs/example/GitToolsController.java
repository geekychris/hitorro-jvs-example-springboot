package com.hitorro.jvs.example;

import com.hitorro.gittools.commits.CommitSelector;
import com.hitorro.gittools.git.GitService;
import com.hitorro.gittools.model.*;
import com.hitorro.gittools.scanner.RepositoryScanner;
import com.hitorro.gittools.scanner.ScanConfig;
import com.hitorro.gittools.tagger.RepositoryTagger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Path;
import java.util.*;

@RestController
@RequestMapping("/api/gittools")
@CrossOrigin(origins = "*")
public class GitToolsController {

    private static final Logger log = LoggerFactory.getLogger(GitToolsController.class);
    private final GitService gitService = new GitService();
    private final RepositoryScanner scanner = new RepositoryScanner(gitService);
    private final CommitSelector commitSelector = new CommitSelector(gitService);
    private final RepositoryTagger tagger;

    private List<GitRepository> cachedRepos = new ArrayList<>();

    public GitToolsController() {
        Path metaFile = Path.of("data/git-metadata.json");
        this.tagger = new RepositoryTagger(metaFile);
    }

    // ─── Scan & List ─────────────────────────────────────────────

    @PostMapping("/scan")
    public ResponseEntity<Map<String, Object>> scan(@RequestBody Map<String, Object> body) {
        @SuppressWarnings("unchecked")
        List<String> roots = (List<String>) body.getOrDefault("roots",
                List.of(System.getProperty("user.home") + "/hitorro"));
        int maxDepth = (int) body.getOrDefault("maxDepth", 4);

        List<Path> rootPaths = roots.stream().map(Path::of).toList();
        var config = new ScanConfig(rootPaths, maxDepth,
                List.of("node_modules", ".m2", "target", "build", "venv", "__pycache__"));

        cachedRepos = scanner.scanWithMetadata(config, tagger);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("count", cachedRepos.size());
        result.put("repos", repoSummaries(cachedRepos));
        return ResponseEntity.ok(result);
    }

    @GetMapping("/repos")
    public ResponseEntity<List<Map<String, Object>>> listRepos(
            @RequestParam(required = false) String tag,
            @RequestParam(required = false) String sort) {
        var filtered = cachedRepos;
        if (tag != null && !tag.isBlank()) {
            filtered = filtered.stream()
                    .filter(r -> r.tags() != null && r.tags().contains(tag))
                    .toList();
        }
        if ("name".equals(sort)) {
            filtered = filtered.stream().sorted(Comparator.comparing(r -> r.name().toLowerCase())).toList();
        } else if ("date".equals(sort)) {
            filtered = filtered.stream().sorted(Comparator.comparing(
                    r -> r.lastCommitDate() != null ? r.lastCommitDate() : java.time.Instant.EPOCH,
                    Comparator.reverseOrder())).toList();
        }
        return ResponseEntity.ok(repoSummaries(filtered));
    }

    @GetMapping("/tags")
    public ResponseEntity<List<String>> allTags() {
        return ResponseEntity.ok(tagger.getAllTags());
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of(
                "cachedRepos", cachedRepos.size(),
                "tags", tagger.getAllTags()));
    }

    // ─── Repo Details ────────────────────────────────────────────

    @GetMapping("/repos/{index}/branches")
    public ResponseEntity<?> branches(@PathVariable int index) {
        if (index < 0 || index >= cachedRepos.size()) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(gitService.listBranches(cachedRepos.get(index).path()));
    }

    @GetMapping("/repos/{index}/tags")
    public ResponseEntity<?> tags(@PathVariable int index) {
        if (index < 0 || index >= cachedRepos.size()) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(gitService.listTags(cachedRepos.get(index).path()));
    }

    @GetMapping("/repos/{index}/commits")
    public ResponseEntity<?> commits(@PathVariable int index,
                                      @RequestParam(defaultValue = "30") int maxCount,
                                      @RequestParam(required = false) String branch,
                                      @RequestParam(required = false) String author,
                                      @RequestParam(required = false) String message) {
        if (index < 0 || index >= cachedRepos.size()) return ResponseEntity.notFound().build();
        var commits = commitSelector.selectCommits(
                cachedRepos.get(index).path(), branch, maxCount, author, message, null, null);
        return ResponseEntity.ok(commits);
    }

    @GetMapping("/repos/{index}/commits/{hash}")
    public ResponseEntity<?> commitDetail(@PathVariable int index, @PathVariable String hash) {
        if (index < 0 || index >= cachedRepos.size()) return ResponseEntity.notFound().build();
        var commit = gitService.getCommit(cachedRepos.get(index).path(), hash);
        if (commit == null) return ResponseEntity.notFound().build();
        var files = gitService.getChangedFiles(cachedRepos.get(index).path(), hash);
        return ResponseEntity.ok(Map.of("commit", commit, "files", files));
    }

    @GetMapping("/repos/{index}/commits/{hash}/diff")
    public ResponseEntity<?> commitDiff(@PathVariable int index, @PathVariable String hash) {
        if (index < 0 || index >= cachedRepos.size()) return ResponseEntity.notFound().build();
        try {
            String diff = gitService.diff(cachedRepos.get(index).path(), hash);
            return ResponseEntity.ok(Map.of("diff", diff));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("error", e.getMessage()));
        }
    }

    // ─── Tagging ─────────────────────────────────────────────────

    @PostMapping("/repos/{index}/tag")
    public ResponseEntity<?> addTag(@PathVariable int index, @RequestBody Map<String, String> body) {
        if (index < 0 || index >= cachedRepos.size()) return ResponseEntity.notFound().build();
        tagger.addTag(cachedRepos.get(index).path(), body.get("tag"));
        return ResponseEntity.ok(Map.of("tags", tagger.getMetadata(cachedRepos.get(index).path()).getTags()));
    }

    @DeleteMapping("/repos/{index}/tag/{tag}")
    public ResponseEntity<?> removeTag(@PathVariable int index, @PathVariable String tag) {
        if (index < 0 || index >= cachedRepos.size()) return ResponseEntity.notFound().build();
        tagger.removeTag(cachedRepos.get(index).path(), tag);
        return ResponseEntity.ok(Map.of("tags", tagger.getMetadata(cachedRepos.get(index).path()).getTags()));
    }

    @PostMapping("/repos/{index}/description")
    public ResponseEntity<?> setDescription(@PathVariable int index, @RequestBody Map<String, String> body) {
        if (index < 0 || index >= cachedRepos.size()) return ResponseEntity.notFound().build();
        tagger.setDescription(cachedRepos.get(index).path(), body.get("description"));
        return ResponseEntity.ok(Map.of("success", true));
    }

    // ─── Helpers ─────────────────────────────────────────────────

    private List<Map<String, Object>> repoSummaries(List<GitRepository> repos) {
        List<Map<String, Object>> list = new ArrayList<>();
        for (int i = 0; i < repos.size(); i++) {
            var r = repos.get(i);
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("index", i);
            info.put("name", r.name());
            info.put("path", r.path().toString());
            info.put("description", r.description());
            info.put("tags", r.tags());
            info.put("currentBranch", r.currentBranch());
            info.put("branchCount", r.branchCount());
            info.put("tagCount", r.tagCount());
            info.put("remoteUrl", r.remoteUrl());
            info.put("lastCommitHash", r.lastCommitHash());
            info.put("lastCommitMessage", r.lastCommitMessage());
            info.put("lastCommitDate", r.lastCommitDate());
            info.put("lastCommitAuthor", r.lastCommitAuthor());
            list.add(info);
        }
        return list;
    }
}
