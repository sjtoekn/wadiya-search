import os

from tests.crawler_fixtures import Env, page, site_routes
from tests.helpers import TempDirCase
from tests.mock_site import MockSite
from wadiya.crawler.seeds import Seed, load_seeds


class CrawlerTests(TempDirCase):
    def run_crawl(self, site, seeds=None, env_kw=None, crawler_kw=None, fresh=False):
        env = Env(self.settings, site, **(env_kw or {}))
        stats = env.crawler(self.storage, **(crawler_kw or {})).run(seeds or [Seed(site.base + "/")], fresh=fresh)
        return stats, env

    def urls(self):
        with self.storage._conn() as c:
            return {r["url"].split("/", 3)[3] if r["url"].count("/") > 2 else "": r["status"]
                    for r in c.execute("SELECT url, status FROM urls")}

    def test_full_crawl_behaviour(self):
        with MockSite(site_routes()) as site:
            stats, _ = self.run_crawl(site)
            # robots.txt respected: the disallowed path was never requested
            self.assertNotIn("/private/secret", site.hits)
            self.assertIn("/robots.txt", site.hits)
            u = self.urls()
            self.assertEqual(u["private/secret"], "robots_blocked")
            self.assertEqual(u["noindex"], "noindex")
            self.assertEqual(u["dup"], "duplicate")
            self.assertEqual(u["missing"], "failed")
            self.assertEqual(u["redir"], "failed")                 # redirect to another site refused
            self.assertNotIn("file.pdf", u)                         # non-HTML extension never queued
            self.assertFalse(any("elsewhere" in k for k in u))      # off-site link never queued
            self.assertEqual(u["deep"], "done")                     # depth 2 reached
            self.assertEqual(site.hits.count("/a"), 1)              # utm_ variant normalised away => fetched once
            # stored documents
            self.assertEqual(self.storage.count_documents(), 4)     # home, a, b, deep
            titles = {d.title for d in self.storage.get_documents(range(1, 10)).values()}
            self.assertEqual(titles, {"Wadiya Home", "Wadi guide", "Oasis facts", "Deep page"})
            blob = " ".join(d.content for d in self.storage.get_documents(range(1, 10)).values())
            self.assertNotIn("SECRETCONTENT", blob); self.assertNotIn("NOINDEXCONTENT", blob)
            # metrics
            self.assertEqual(stats.robots_blocked, 1)
            self.assertEqual(stats.duplicates, 1)
            self.assertEqual(stats.stored, 4)
            self.assertEqual(stats.failed, 2)
            self.assertEqual(stats.attempted, stats.fetched + stats.robots_blocked + stats.failed)
            run = self.storage.last_run()
            self.assertEqual((run["state"], run["stored"]), ("finished", 4))
            self.assertTrue(self.storage.recent_crawl_errors())

    def test_canonical_url_is_discovered_and_crawled(self):
        routes = site_routes()
        routes["/"] = (
            200,
            *page(
                "Home",
                "Home page that points to a canonical destination.",
                '<link rel="canonical" href="/canonical-destination">',
            ),
        )
        routes["/canonical-destination"] = (
            200,
            *page("Canonical", "The canonical destination was discovered."),
        )

        with MockSite(routes) as site:
            stats, _ = self.run_crawl(
                site,
                env_kw={"max_depth": 1},
            )

            self.assertIn("/canonical-destination", site.hits)
            self.assertIn(
                "Canonical",
                {d.title for d in self.storage.get_documents(range(1, 20)).values()},
            )
            self.assertGreaterEqual(stats.stored, 2)

    def test_queue_growth_bound_is_respected(self):
        routes = site_routes()
        links = [f"/generated/{i}" for i in range(20)]
        routes["/"] = (
            200,
            *page(
                "Home",
                "Page containing many discovered URLs.",
                links=links,
            ),
        )
        for i in range(20):
            routes[f"/generated/{i}"] = (
                200,
                *page(f"Generated {i}", f"Generated page {i}."),
            )

        with MockSite(routes) as site:
            self.run_crawl(
                site,
                seeds=[Seed(site.base + "/", max_pages=2)],
                env_kw={"max_depth": 1},
            )

            with self.storage._conn() as c:
                count = c.execute(
                    "SELECT count(*) FROM urls WHERE domain=?",
                    ("127.0.0.1",),
                ).fetchone()[0]

            self.assertLessEqual(count, 10)

    def test_percent_encoded_robots_rule_is_honoured_end_to_end(self):
        """Regression for the %7E bug: crawler normalises links, robots rule is written in encoded form."""
        routes = site_routes()
        routes["/robots.txt"] = (200, "text/plain", "User-agent: *\nDisallow: /%7Ehidden/\n", {})
        routes["/"] = (200, *page("Home", "Home page with a link to a disallowed user directory.", links=["/%7Ehidden/page", "/~hidden/other", "/a"]))
        routes["/~hidden/page"] = (200, *page("Hidden", "HIDDENCONTENT"))
        with MockSite(routes) as site:
            stats, _ = self.run_crawl(site, env_kw={"max_depth": 1})
            self.assertFalse([h for h in site.hits if "hidden" in h], site.hits)     # neither spelling was ever requested
            self.assertEqual(stats.robots_blocked, 2)                                # /%7Ehidden/page (normalised to ~) and /~hidden/other
            self.assertNotIn("HIDDENCONTENT", " ".join(d.content for d in self.storage.get_documents(range(1, 9)).values()))

    def test_crawl_delay_is_honoured(self):
        routes = site_routes(); routes["/robots.txt"] = (200, "text/plain", "User-agent: *\nCrawl-delay: 3\n", {})
        sleeps, now = [], [0.0]

        def fake_sleep(s): sleeps.append(s); now[0] += s
        with MockSite(routes) as site:
            self.run_crawl(site, env_kw={"max_depth": 1}, crawler_kw={"clock": lambda: now[0], "sleep": fake_sleep})
        self.assertTrue(sleeps)
        self.assertTrue(all(s >= 2.9 for s in sleeps), sleeps)

    def test_crawl_delay_capped(self):
        routes = site_routes(); routes["/robots.txt"] = (200, "text/plain", "User-agent: *\nCrawl-delay: 9999\n", {})
        sleeps, now = [], [0.0]

        def fake_sleep(s): sleeps.append(s); now[0] += s
        with MockSite(routes) as site:
            self.run_crawl(site, env_kw={"max_depth": 1, "max_crawl_delay": 5.0},
                           crawler_kw={"clock": lambda: now[0], "sleep": fake_sleep})
        self.assertTrue(sleeps and max(sleeps) <= 5.0)

    def test_depth_limit(self):
        with MockSite(site_routes()) as site:
            self.run_crawl(site, seeds=[Seed(site.base + "/", max_depth=0)])
            self.assertEqual(self.storage.count_documents(), 1)
            self.assertNotIn("/a", site.hits)

    def test_page_limit_per_domain(self):
        with MockSite(site_routes()) as site:
            self.run_crawl(site, seeds=[Seed(site.base + "/", max_pages=2)])
            self.assertEqual(self.storage.count_documents(), 2)
            self.assertIn("domain_page_limit", {r["error"] for r in self._log()})

    def test_total_page_cap(self):
        with MockSite(site_routes()) as site:
            stats, _ = self.run_crawl(site, env_kw={"max_total_pages": 2})
            self.assertLessEqual(stats.attempted, 2)

    def test_path_prefix_scope(self):
        routes = site_routes(); routes["/docs/x"] = (200, *page("Doc X", "Inside docs prefix."))
        routes["/docs/"] = (200, *page("Docs", "Docs home.", links=["/docs/x", "/a"]))
        with MockSite(routes) as site:
            self.run_crawl(site, seeds=[Seed(site.base + "/docs/", path_prefix="/docs/")])
            self.assertEqual({d.title for d in self.storage.get_documents(range(1, 9)).values()}, {"Docs", "Doc X"})
            self.assertNotIn("/a", site.hits)

    def test_robots_unreachable_means_no_crawling(self):
        routes = site_routes(); routes["/robots.txt"] = (503, "text/plain", "down", {})
        with MockSite(routes) as site:
            stats, _ = self.run_crawl(site)
            self.assertEqual(self.storage.count_documents(), 0)
            self.assertNotIn("/", [h for h in site.hits if h != "/robots.txt"])
            self.assertEqual(stats.robots_blocked, 1)

    def test_missing_robots_allows_crawling(self):
        routes = site_routes(); del routes["/robots.txt"]
        with MockSite(routes) as site:
            self.run_crawl(site)
            self.assertIn("/private/secret", site.hits)           # no robots.txt => nothing disallowed
            self.assertGreaterEqual(self.storage.count_documents(), 4)

    def test_recrawl_is_idempotent(self):
        with MockSite(site_routes()) as site:
            self.run_crawl(site)
            before = self.storage.count_documents()
            stats, _ = self.run_crawl(site, fresh=True)
            self.assertEqual(self.storage.count_documents(), before)
            self.assertEqual(stats.stored, 0)                       # all unchanged
            self.assertEqual(self.storage.documents_needing_index(50).__len__(), before)  # nothing re-marked dirty by crawl

    def test_invalid_seed_and_unsafe_seed(self):
        with MockSite(site_routes()) as site:
            stats, _ = self.run_crawl(site, seeds=[Seed("javascript:alert(1)"), Seed("ftp://x/"), Seed(site.base + "/")])
            self.assertGreater(stats.stored, 0)
        # default (secure) policy: a local seed is refused at fetch time
        from wadiya.config.settings import Settings
        from wadiya.crawler.crawler import Crawler
        from wadiya.crawler.fetcher import Fetcher
        from wadiya.crawler.robots import RobotsChecker
        docs_before = self.storage.count_documents()
        with MockSite(site_routes()) as site:
            s = Settings(**{**self.settings.__dict__, "default_crawl_delay": 0.0, "max_retries": 0})
            f = Fetcher(s)
            Crawler(s, self.storage, f, RobotsChecker(f, s.robots_token)).run([Seed(site.base + "/")], fresh=True)
            self.assertEqual(site.hits, [])
            self.assertEqual(self.storage.count_documents(), docs_before)   # nothing new stored
            self.assertTrue(any("unsafe_url" in (r["error"] or "") for r in self._log()))

    def test_seed_file_loading(self):
        p = os.path.join(self.tmp, "seeds.yaml")
        with open(p, "w") as f:
            f.write("seeds:\n  - url: https://a.org/\n    max_pages: 5\n  - https://b.org/docs/\n")
        seeds = load_seeds(p)
        self.assertEqual((seeds[0].max_pages, seeds[1].url), (5, "https://b.org/docs/"))

    def _log(self):
        with self.storage._conn() as c:
            return [dict(r) for r in c.execute("SELECT * FROM crawl_log")]
