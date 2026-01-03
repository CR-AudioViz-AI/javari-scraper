// Javari Scraper - Status Endpoint
// =================================
// GET /api/status - Check scraper status and available sources

export default async function handler(req, res) {
  const SCRAPERS = {
    spirits: {
      table: 'bv_spirits',
      sources: [
        'ttb_cola',      // NEW! 500K+ US alcohol labels
        'openfoodfacts', 
        'brewery', 
        'punkapi', 
        'cocktaildb', 
        'untappd'
      ],
      description: 'Spirits, beer, wine, cocktails - NOW WITH TTB COLA (500K+)',
    },
    cards: {
      table: 'cards',
      sources: ['pokemon', 'scryfall'],
      description: 'Trading cards (Pokemon, MTG)',
    },
    books: {
      table: 'books',
      sources: ['openlibrary', 'gutenberg'],
      description: 'Books and ebooks',
    },
  };
  
  // Check Supabase connection
  let supabaseStatus = 'not_configured';
  if (process.env.SUPABASE_SERVICE_KEY) {
    try {
      const response = await fetch(
        `${process.env.SUPABASE_URL}/rest/v1/bv_spirits?select=id&limit=1`,
        {
          headers: {
            'apikey': process.env.SUPABASE_SERVICE_KEY,
            'Authorization': `Bearer ${process.env.SUPABASE_SERVICE_KEY}`,
          },
        }
      );
      supabaseStatus = response.ok ? 'connected' : 'error';
    } catch {
      supabaseStatus = 'error';
    }
  }
  
  // Check Untappd credentials
  const untappdStatus = process.env.UNTAPPD_CLIENT_ID ? 'configured' : 'not_configured';
  
  return res.status(200).json({
    status: 'ok',
    version: '1.1.0',
    timestamp: new Date().toISOString(),
    scrapers: SCRAPERS,
    connections: {
      supabase: supabaseStatus,
      untappd: untappdStatus,
    },
    usage: {
      spirits: 'GET /api/scrape?type=spirits&source=all',
      ttb_cola: 'GET /api/scrape?type=spirits&source=ttb_cola (500K+ products!)',
      cards: 'GET /api/scrape?type=cards&source=pokemon',
      books: 'GET /api/scrape?type=books&source=openlibrary',
      skip_upload: 'Add &skip_upload=true to test without uploading',
      limit: 'Add &limit=1000 to limit results',
    },
    cron: {
      schedule: '0 3 * * *',
      description: 'Runs daily at 3 AM UTC',
    },
  });
}
