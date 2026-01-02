# Javari Universal Data Scraper

A reusable scraper framework for the Javari ecosystem. Automatically scrapes and uploads data for:
- **Spirits** (CravBarrels/Javari Spirits) - 30K+ products
- **Trading Cards** (Javari Cards) - 100K+ cards  
- **Books** (Javari Books) - 100K+ books

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/status` | Health check and available sources |
| `GET /api/scrape?type=spirits&source=all` | Scrape all spirits sources |
| `GET /api/scrape?type=cards&source=pokemon` | Scrape Pokemon cards |
| `GET /api/scrape?type=books&source=openlibrary` | Scrape Open Library |

## Automated Schedule (Cron)

| Type | Schedule | Sources |
|------|----------|---------|
| Spirits | Daily 3 AM UTC | OpenFoodFacts, Brewery, PunkAPI, CocktailDB |
| Cards | Weekly Sunday 4-5 AM UTC | Pokemon TCG, Scryfall |
| Books | Weekly Sunday 6 AM UTC | Open Library |

## Environment Variables

Set these in Vercel Dashboard → Settings → Environment Variables:

```
SUPABASE_URL=https://pvxsazjqfdhisczwqnsv.supabase.co
SUPABASE_SERVICE_KEY=your_service_key
UNTAPPD_CLIENT_ID=your_client_id (optional)
UNTAPPD_CLIENT_SECRET=your_client_secret (optional)
SCRAPER_SECRET=optional_auth_token
```

## Data Sources

### Spirits (bv_spirits table)
- Open Food Facts - 20K+ alcoholic beverages
- Open Brewery DB - 9K+ US breweries
- PunkAPI - 300+ BrewDog beers
- TheCocktailDB - 600+ cocktails
- Untappd - 8M+ beers (requires API key)

### Trading Cards (cards table)
- Pokemon TCG API - 15K+ Pokemon cards
- Scryfall - 80K+ MTG cards

### Books (books table)
- Open Library - 50K+ books
- Project Gutenberg - 70K+ free ebooks

## Manual Trigger

```bash
# Scrape all spirits
curl https://javari-scraper.vercel.app/api/scrape?type=spirits&source=all

# Scrape specific source
curl https://javari-scraper.vercel.app/api/scrape?type=spirits&source=openfoodfacts

# Test without uploading
curl https://javari-scraper.vercel.app/api/scrape?type=spirits&source=punkapi&skip_upload=true
```

## Adding New Scrapers

Edit `api/scrape.js` and add to the `SCRAPERS` object:

```javascript
SCRAPERS.mytype = {
  table: 'my_table',
  sources: {
    mysource: {
      name: 'My Source',
      fn: scrapeMySource,
      schedule: 'daily',
      estimated: 10000,
    },
  },
  transform: transformMyType,
};
```

## License

Proprietary - CR AudioViz AI LLC
