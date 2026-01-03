export default function handler(req, res) {
  res.status(200).json({
    service: "Javari Scraper",
    version: "1.0.0",
    status: "operational",
    description: "Universal data scraper for Javari ecosystem",
    endpoints: {
      status: "/api/status",
      scrape: "/api/scrape?type={spirits|cards|books}&source={all|pokemon|scryfall|openlibrary}"
    },
    scheduledJobs: [
      { path: "/api/scrape?type=spirits&source=all", schedule: "Daily 3 AM UTC" },
      { path: "/api/scrape?type=cards&source=pokemon", schedule: "Weekly Sunday 4 AM UTC" },
      { path: "/api/scrape?type=cards&source=scryfall", schedule: "Weekly Sunday 5 AM UTC" },
      { path: "/api/scrape?type=books&source=openlibrary", schedule: "Weekly Sunday 6 AM UTC" }
    ],
    timestamp: new Date().toISOString()
  });
}
