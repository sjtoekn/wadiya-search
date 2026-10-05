"""Crawl orchestration. Fetch -> robots -> extract -> store. No ranking, no index access."""
from __future__ import annotations

import hashlib
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from urllib.parse import urlsplit

from ..storage.base import Document, Storage, now_iso
from .extractor import extract
from .lock import CrawlLock
from .robots import parse_x_robots_tag
from .urls import looks_crawlable, normalize_url, site_key


@dataclass
class CrawlStats:
    attempted: int = 0          # URLs we tried to crawl (after scope/limit checks)
    fetched: int = 0            # successfully fetched + extracted
    robots_blocked: int = 0
    failed: int = 0
    stored: int = 0
    duplicates: int = 0
    deactivated: int = 0        # previously searchable documents removed from search this run
    fetch_ms_total: float = 0.0
    fetch_count: int = 0
    duration_s: float = 0.0

    def as_dict(self) -> dict:
        d = dict(self.__dict__)
        d["avg_fetch_ms"] = round(self.fetch_ms_total / self.fetch_count, 1) if self.fetch_count else None
        return d


class Crawler:
    def __init__(self, settings, storage: Storage, fetcher, robots, *, clock=time.monotonic, sleep=time.sleep,
                 lock_path=None):
        self.s, self.storage, self.fetcher, self.robots = settings, storage, fetcher, robots
        self.lock_path = lock_path or (settings.db_path + ".crawl.lock")
        self._clock, self._sleep = clock, sleep
        self._ready_at = {}

    # ------------------------------------------------------------------ run loop
    def run(self, seeds, fresh: bool = False) -> CrawlStats:
        """Runs a crawl. Raises CrawlAlreadyRunning (touching nothing) if another crawler holds the lock."""
        with CrawlLock(self.lock_path):
            return self._run(seeds, fresh)

    def _run(self, seeds, fresh: bool) -> CrawlStats:
        from .seeds import Scope

        scope = Scope(seeds, self.s)
        self._ready_at = {}
        self.storage.requeue_stale()
        if fresh:
            self.storage.reset_queue()

        run_id = self.storage.start_run()
        stats, t0, state = CrawlStats(), self._clock(), "finished"

        try:
            for seed in seeds:
                u = normalize_url(seed.url)
                if not u or not scope.allows(u):
                    self.storage.log_crawl_event(
                        run_id, str(seed.url), "failed", None, None, "invalid_seed"
                    )
                    continue
                self.storage.enqueue(
                    u, site_key(urlsplit(u).hostname), 0, force=True
                )

            processed = 0
            active_domains = set()
            futures = {}

            with ThreadPoolExecutor(max_workers=max(1, self.s.max_workers)) as executor:
                while processed < self.s.max_total_pages or futures:
                    # Collect completed workers first.
                    completed = [
                        future for future in futures
                        if future.done()
                    ]

                    for future in completed:
                        domain = futures.pop(future)
                        active_domains.discard(domain)
                        self._merge_stats(stats, future.result())

                    # Fill available worker slots without exceeding the
                    # global page limit.
                    while (
                        processed < self.s.max_total_pages
                        and len(futures) < max(1, self.s.max_workers)
                    ):
                        now = self._clock()
                        blocked = {
                            domain
                            for domain, ready_at in self._ready_at.items()
                            if ready_at > now
                        }
                        excluded = blocked | active_domains

                        item = self.storage.claim_next(excluded)
                        if item is None:
                            break

                        processed += 1
                        active_domains.add(item.domain)
                        future = executor.submit(
                            self._process_worker,
                            item,
                            scope,
                            run_id,
                        )
                        futures[future] = item.domain

                    if futures:
                        # Wait until at least one worker finishes instead of
                        # busy-polling. Other domains continue concurrently.
                        done, _ = wait(
                            futures,
                            return_when=FIRST_COMPLETED,
                        )
                        for future in done:
                            domain = futures.pop(future)
                            active_domains.discard(domain)
                            self._merge_stats(stats, future.result())
                        continue

                    if processed >= self.s.max_total_pages:
                        break

                    now = self._clock()
                    blocked = [
                        ready_at
                        for ready_at in self._ready_at.values()
                        if ready_at > now
                    ]

                    if self.storage.queue_size() == 0:
                        break

                    if blocked:
                        self._sleep(max(min(blocked) - now, 0.01))
                        continue

                    # Nothing is active, nothing is delayed, but no item
                    # could be claimed. Avoid a busy loop.
                    self._sleep(0.01)

                # Drain any workers still finishing after the page limit.
                for future, domain in list(futures.items()):
                    self._merge_stats(stats, future.result())
                    active_domains.discard(domain)

            stats.duration_s = round(self._clock() - t0, 2)
            self.storage.update_run(run_id, stats.as_dict())

        except KeyboardInterrupt:
            state = "interrupted"
            raise
        finally:
            stats.duration_s = round(self._clock() - t0, 2)
            self.storage.finish_run(run_id, stats.as_dict(), state)

        return stats

    # ------------------------------------------------------------------ worker
    def _process_worker(self, item, scope, run_id):
        """Process one claimed URL in a worker and return isolated stats."""
        stats = CrawlStats()
        try:
            self._process(item, scope, stats, run_id)
        except Exception as e:
            stats.failed += 1
            self._finish(
                run_id,
                item.url,
                "failed",
                "failed",
                error=f"internal_error: {type(e).__name__}",
            )
        return stats

    def _merge_stats(self, target: CrawlStats, source: CrawlStats):
        """Merge worker-local counters into the crawl-wide stats."""
        target.attempted += source.attempted
        target.fetched += source.fetched
        target.robots_blocked += source.robots_blocked
        target.failed += source.failed
        target.stored += source.stored
        target.duplicates += source.duplicates
        target.deactivated += source.deactivated
        target.fetch_ms_total += source.fetch_ms_total
        target.fetch_count += source.fetch_count

    # ------------------------------------------------------------------ one URL
    def _finish(self, run_id, url, outcome, queue_status, http=None, ms=None, error=None):
        self.storage.mark_url(url, queue_status, http, error)
        self.storage.log_crawl_event(run_id, url, outcome, http, ms, error)

    def _delay_for(self, url) -> float:
        d = max(self.s.default_crawl_delay, self.robots.crawl_delay(url) or 0.0)
        return min(d, self.s.max_crawl_delay)

    def _deactivate(self, urls, reason, stats, by_canonical=True) -> bool:
        ids = self.storage.deactivate_document(urls, reason, by_canonical)
        stats.deactivated += len(ids)
        return bool(ids)

    def _process(self, item, scope, stats: CrawlStats, run_id):
        url, site = item.url, item.domain
        cfg = scope.config(site)
        if cfg is None or not scope.allows(url):
            return self._finish(run_id, url, "skipped", "skipped", error="out_of_scope")
        if self.storage.domain_crawled_count(site) >= cfg.max_pages:
            return self._finish(run_id, url, "skipped", "skipped", error="domain_page_limit")

        stats.attempted += 1
        unsafe = self.fetcher.validate(url)      # report the true reason before robots.txt is even consulted
        if unsafe:
            stats.failed += 1
            stats.deactivated += len(self.storage.record_fetch_failure([url], self.s.max_consecutive_failures))
            return self._finish(run_id, url, "failed", "failed", error=unsafe)
        if not self.robots.is_allowed(url):
            stats.robots_blocked += 1
            note = "disallowed by robots.txt"
            # only an affirmative Disallow removes a page; an unreadable robots.txt must not wipe good documents
            if self.robots.block_reason(url) == "rules" and self._deactivate([url], "robots", stats):
                note += " (removed from index)"
            return self._finish(run_id, url, "robots_blocked", "robots_blocked", error=note)

        res = self.fetcher.fetch(url, redirect_filter=scope.allows)
        self._ready_at[site] = self._clock() + self._delay_for(url)
        stats.fetch_ms_total += res.elapsed_ms
        stats.fetch_count += 1
        if not res.ok:
            stats.failed += 1
            err = res.error or f"http_{res.status}"
            if res.error is None and res.status in (404, 410):        # definitive: the page is gone
                if self._deactivate([url], "gone", stats):
                    err += " (removed from index)"
            else:                                                       # possibly temporary: keep the doc for now
                dropped = self.storage.record_fetch_failure([url], self.s.max_consecutive_failures)
                stats.deactivated += len(dropped)
                if dropped:
                    err += f" (removed from index after {self.s.max_consecutive_failures} failed crawls)"
            return self._finish(run_id, url, "failed", "failed", res.status, res.elapsed_ms, err)

        final = normalize_url(res.final_url) or url
        try:
            page = extract(res.body, final, res.content_type)
        except Exception as e:
            stats.failed += 1
            stats.deactivated += len(self.storage.record_fetch_failure([url], self.s.max_consecutive_failures))
            return self._finish(run_id, url, "failed", "failed", res.status, res.elapsed_ms,
                                f"extract_error: {type(e).__name__}")
        stats.fetched += 1
        final_site = site_key(urlsplit(final).hostname)

        hdr_noindex, hdr_nofollow = parse_x_robots_tag(res.x_robots_tag, self.s.robots_token)
        noindex, nofollow = page.noindex or hdr_noindex, page.nofollow or hdr_nofollow

        # canonical: honoured only when it stays on the same site (a page cannot claim another site's identity)
        canonical = final
        if page.canonical_url and site_key(urlsplit(page.canonical_url).hostname) == final_site:
            canonical = page.canonical_url
        links = [] if nofollow else list(page.links)
        if canonical != final and not nofollow:
            links.append(canonical)
        self._enqueue_links(links, item, cfg, scope)

        if noindex:
            removed = self._deactivate([url, final, canonical], "noindex", stats)
            return self._finish(run_id, url, "noindex", "noindex", res.status, res.elapsed_ms,
                                "removed from index" if removed else None)
        content = page.content[: self.s.max_content_chars]
        if not content.strip():
            removed = self._deactivate([url, final, canonical], "empty", stats)
            return self._finish(run_id, url, "skipped", "skipped", res.status, res.elapsed_ms,
                                "empty_content" + (" (removed from index)" if removed else ""))
        if final != url:        # the requested URL now redirects: it no longer serves the page itself
            self._deactivate([url], "moved", stats, by_canonical=False)

        now = now_iso()
        doc = Document(id=None, url=final, canonical_url=canonical, domain=final_site, title=page.title,
                       description=page.description, content=content, language=page.language, status="ok",
                       content_hash=hashlib.sha256(content.encode("utf-8")).hexdigest(),
                       crawled_at=now, updated_at=now, headings="\n".join(page.headings))
        saved = self.storage.save_document(doc)
        if saved.outcome == "duplicate":
            stats.duplicates += 1
            return self._finish(run_id, url, "duplicate", "duplicate", res.status, res.elapsed_ms)
        if saved.outcome in ("inserted", "updated", "reactivated"):
            stats.stored += 1
        self._finish(run_id, url, "stored" if saved.outcome == "inserted" else saved.outcome, "done",
                     res.status, res.elapsed_ms)

    def _enqueue_links(self, links, item, cfg, scope):
        if item.depth >= cfg.max_depth:
            return
        for link in links:
            if not looks_crawlable(link) or not scope.allows(link):
                continue
            site = site_key(urlsplit(link).hostname)
            site_cfg = scope.config(site)
            if self.storage.domain_url_count(site) >= site_cfg.max_pages * 5:   # bound queue growth
                continue
            self.storage.enqueue(link, site, item.depth + 1)
