// Javari Universal Data Scraper
// ==============================
// A reusable scraper framework for any data type
// 
// Endpoints:
//   GET /api/scrape?type=spirits&source=all
//   GET /api/scrape?type=cards&source=pokemon
//   GET /api/scrape?type=books&source=openlibrary
//   POST /api/scrape/custom - Custom scrape config
//
// Author: Javari AI for CR AudioViz AI LLC
// Created: 2026-01-02

// =============================================================================
// CONFIGURATION
// =============================================================================

const CONFIG = {
  supabase: {
    url: process.env.SUPABASE_URL || 'https://pvxsazjqfdhisczwqnsv.supabase.co',
    serviceKey: process.env.SUPABASE_SERVICE_KEY,
  },
  untappd: {
    clientId: process.env.UNTAPPD_CLIENT_ID,
    clientSecret: process.env.UNTAPPD_CLIENT_SECRET,
  },
  scraperSecret: process.env.SCRAPER_SECRET,
};

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function fetchWithRetry(url, options = {}, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url, { ...options, timeout: 30000 });
      if (response.ok) return response;
      if (response.status === 429) {
        await delay(5000 * (i + 1)); // Exponential backoff
        continue;
      }
      throw new Error(`HTTP ${response.status}`);
    } catch (error) {
      if (i === retries - 1) throw error;
      await delay(1000 * (i + 1));
    }
  }
}

// =============================================================================
// SCRAPER REGISTRY - Add new scrapers here
// =============================================================================

const SCRAPERS = {
  // -------------------------------------------------------------------------
  // SPIRITS
  // -------------------------------------------------------------------------
  spirits: {
    table: 'bv_spirits',
    sources: {
      openfoodfacts: {
        name: 'Open Food Facts',
        fn: scrapeOpenFoodFacts,
        schedule: 'daily',
        estimated: 20000,
      },
      brewery: {
        name: 'Open Brewery DB',
        fn: scrapeOpenBreweryDB,
        schedule: 'weekly',
        estimated: 9000,
      },
      punkapi: {
        name: 'PunkAPI (BrewDog)',
        fn: scrapePunkAPI,
        schedule: 'weekly',
        estimated: 300,
      },
      cocktaildb: {
        name: 'TheCocktailDB',
        fn: scrapeCocktailDB,
        schedule: 'weekly',
        estimated: 600,
      },
      untappd: {
        name: 'Untappd',
        fn: scrapeUntappd,
        schedule: 'daily',
        estimated: 1000, // per run due to rate limits
        requiresAuth: true,
      },
    },
    transform: transformSpirit,
  },
  
  // -------------------------------------------------------------------------
  // TRADING CARDS (for javari-cards)
  // -------------------------------------------------------------------------
  cards: {
    table: 'cards',
    sources: {
      pokemon: {
        name: 'Pokemon TCG API',
        fn: scrapePokemonTCG,
        schedule: 'weekly',
        estimated: 15000,
      },
      scryfall: {
        name: 'Scryfall (MTG)',
        fn: scrapeScryfall,
        schedule: 'weekly',
        estimated: 80000,
      },
    },
    transform: transformCard,
  },
  
  // -------------------------------------------------------------------------
  // BOOKS (for javari-books)
  // -------------------------------------------------------------------------
  books: {
    table: 'books',
    sources: {
      openlibrary: {
        name: 'Open Library',
        fn: scrapeOpenLibrary,
        schedule: 'weekly',
        estimated: 50000,
      },
      gutenberg: {
        name: 'Project Gutenberg',
        fn: scrapeGutenberg,
        schedule: 'monthly',
        estimated: 70000,
      },
    },
    transform: transformBook,
  },
};

// =============================================================================
// SPIRITS SCRAPERS
// =============================================================================

async function scrapeOpenFoodFacts(options = {}) {
  const limit = options.limit || 10000;
  const products = [];
  
  const categories = [
    'en:spirits', 'en:whiskies', 'en:vodkas', 'en:rums',
    'en:gins', 'en:tequilas', 'en:brandies', 'en:liqueurs'
  ];
  
  for (const category of categories) {
    let page = 1;
    const maxPerCategory = Math.floor(limit / categories.length);
    let categoryCount = 0;
    
    while (categoryCount < maxPerCategory && page <= 20) {
      try {
        const url = new URL('https://world.openfoodfacts.org/cgi/search.pl');
        url.searchParams.set('action', 'process');
        url.searchParams.set('tagtype_0', 'categories');
        url.searchParams.set('tag_contains_0', 'contains');
        url.searchParams.set('tag_0', category);
        url.searchParams.set('page_size', '100');
        url.searchParams.set('page', page.toString());
        url.searchParams.set('json', '1');
        url.searchParams.set('fields', 'code,product_name,brands,alcohol_100g,image_front_url,quantity,countries');
        
        const response = await fetchWithRetry(url.toString());
        const data = await response.json();
        const items = data.products || [];
        
        if (items.length === 0) break;
        
        for (const item of items) {
          if (item.product_name) {
            products.push({
              name: item.product_name,
              brand: (item.brands || '').split(',')[0],
              category: mapSpiritCategory(category),
              size: item.quantity || '',
              abv: item.alcohol_100g || null,
              image_url: item.image_front_url || null,
              upc: item.code || null,
              country: (item.countries || '').split(',')[0],
              source: 'openfoodfacts',
            });
            categoryCount++;
          }
        }
        
        page++;
        await delay(300);
      } catch (error) {
        console.error(`OFF error (${category}): ${error.message}`);
        break;
      }
    }
  }
  
  return products;
}

async function scrapeOpenBreweryDB(options = {}) {
  const breweries = [];
  let page = 1;
  
  while (page <= 50) {
    try {
      const response = await fetchWithRetry(
        `https://api.openbrewerydb.org/v1/breweries?per_page=200&page=${page}`
      );
      const data = await response.json();
      
      if (!data || data.length === 0) break;
      
      for (const item of data) {
        breweries.push({
          name: item.name,
          brand: item.name,
          category: 'beer',
          country: 'USA',
          region: `${item.city || ''}, ${item.state || ''}`,
          distillery: item.name,
          source: 'openbrewerydb',
          source_url: item.website_url || null,
        });
      }
      
      page++;
      await delay(200);
    } catch (error) {
      console.error(`Brewery error: ${error.message}`);
      break;
    }
  }
  
  return breweries;
}

async function scrapePunkAPI(options = {}) {
  const beers = [];
  let page = 1;
  
  while (page <= 10) {
    try {
      const response = await fetchWithRetry(
        `https://api.punkapi.com/v2/beers?page=${page}&per_page=80`
      );
      const data = await response.json();
      
      if (!data || data.length === 0) break;
      
      for (const item of data) {
        beers.push({
          name: item.name,
          brand: 'BrewDog',
          category: 'beer',
          abv: item.abv || null,
          description: item.description || '',
          image_url: item.image_url || null,
          source: 'punkapi',
        });
      }
      
      page++;
      await delay(300);
    } catch (error) {
      console.error(`PunkAPI error: ${error.message}`);
      break;
    }
  }
  
  return beers;
}

async function scrapeCocktailDB(options = {}) {
  const items = [];
  
  // Get ingredients
  try {
    const response = await fetchWithRetry(
      'https://www.thecocktaildb.com/api/json/v1/1/list.php?i=list'
    );
    const data = await response.json();
    
    for (const drink of (data.drinks || [])) {
      if (drink.strIngredient1) {
        items.push({
          name: drink.strIngredient1,
          category: 'other',
          source: 'thecocktaildb',
        });
      }
    }
  } catch (error) {
    console.error(`CocktailDB ingredients error: ${error.message}`);
  }
  
  // Get cocktails by letter
  const letters = 'abcdefghijklmnopqrstuvwxyz'.split('');
  for (const letter of letters) {
    try {
      const response = await fetchWithRetry(
        `https://www.thecocktaildb.com/api/json/v1/1/search.php?f=${letter}`
      );
      const data = await response.json();
      
      for (const drink of (data.drinks || [])) {
        items.push({
          name: drink.strDrink,
          category: 'other',
          description: drink.strInstructions || '',
          image_url: drink.strDrinkThumb || null,
          source: 'thecocktaildb',
        });
      }
      
      await delay(200);
    } catch (error) {
      continue;
    }
  }
  
  return items;
}

async function scrapeUntappd(options = {}) {
  const { clientId, clientSecret } = CONFIG.untappd;
  
  if (!clientId || !clientSecret) {
    console.log('Untappd: No API credentials');
    return [];
  }
  
  const beers = [];
  const styles = ['IPA', 'Stout', 'Porter', 'Lager', 'Pilsner', 'Wheat', 'Pale Ale', 'Sour'];
  
  for (const style of styles) {
    try {
      const url = new URL('https://api.untappd.com/v4/search/beer');
      url.searchParams.set('client_id', clientId);
      url.searchParams.set('client_secret', clientSecret);
      url.searchParams.set('q', style);
      url.searchParams.set('limit', '50');
      
      const response = await fetchWithRetry(url.toString());
      const data = await response.json();
      
      if (data?.meta?.code === 200) {
        const items = data.response?.beers?.items || [];
        
        for (const item of items) {
          const beer = item.beer || {};
          const brewery = item.brewery || {};
          
          beers.push({
            name: beer.beer_name,
            brand: brewery.brewery_name,
            category: 'beer',
            subcategory: beer.beer_style || '',
            abv: beer.beer_abv || null,
            description: beer.beer_description || '',
            image_url: beer.beer_label || null,
            country: brewery.country_name || '',
            region: brewery.location?.brewery_city || '',
            distillery: brewery.brewery_name,
            source: 'untappd',
          });
        }
      }
      
      // Respect rate limits: 100/hour = 36 seconds between calls
      await delay(40000);
      
    } catch (error) {
      console.error(`Untappd error: ${error.message}`);
    }
  }
  
  return beers;
}

// =============================================================================
// TRADING CARDS SCRAPERS
// =============================================================================

async function scrapePokemonTCG(options = {}) {
  const cards = [];
  let page = 1;
  
  while (page <= 100) {
    try {
      const response = await fetchWithRetry(
        `https://api.pokemontcg.io/v2/cards?page=${page}&pageSize=250`
      );
      const data = await response.json();
      
      if (!data.data || data.data.length === 0) break;
      
      for (const card of data.data) {
        cards.push({
          name: card.name,
          set_name: card.set?.name || '',
          set_id: card.set?.id || '',
          number: card.number || '',
          rarity: card.rarity || '',
          types: (card.types || []).join(', '),
          hp: card.hp || null,
          image_url: card.images?.large || card.images?.small || null,
          price_market: card.tcgplayer?.prices?.holofoil?.market || 
                        card.tcgplayer?.prices?.normal?.market || null,
          source: 'pokemontcg',
        });
      }
      
      page++;
      await delay(500);
    } catch (error) {
      console.error(`Pokemon TCG error: ${error.message}`);
      break;
    }
  }
  
  return cards;
}

async function scrapeScryfall(options = {}) {
  const cards = [];
  let url = 'https://api.scryfall.com/cards/search?q=*&unique=cards';
  
  while (url && cards.length < (options.limit || 50000)) {
    try {
      const response = await fetchWithRetry(url);
      const data = await response.json();
      
      for (const card of (data.data || [])) {
        cards.push({
          name: card.name,
          set_name: card.set_name || '',
          set_id: card.set || '',
          number: card.collector_number || '',
          rarity: card.rarity || '',
          mana_cost: card.mana_cost || '',
          type_line: card.type_line || '',
          image_url: card.image_uris?.normal || card.image_uris?.small || null,
          price_usd: card.prices?.usd || null,
          source: 'scryfall',
        });
      }
      
      url = data.has_more ? data.next_page : null;
      await delay(100); // Scryfall requests 50-100ms between calls
    } catch (error) {
      console.error(`Scryfall error: ${error.message}`);
      break;
    }
  }
  
  return cards;
}

// =============================================================================
// BOOKS SCRAPERS
// =============================================================================

async function scrapeOpenLibrary(options = {}) {
  const books = [];
  const subjects = ['fiction', 'fantasy', 'science_fiction', 'mystery', 'romance', 'history'];
  
  for (const subject of subjects) {
    let offset = 0;
    const limit = 100;
    
    while (offset < 1000) {
      try {
        const response = await fetchWithRetry(
          `https://openlibrary.org/subjects/${subject}.json?limit=${limit}&offset=${offset}`
        );
        const data = await response.json();
        
        if (!data.works || data.works.length === 0) break;
        
        for (const work of data.works) {
          books.push({
            title: work.title,
            author: work.authors?.[0]?.name || '',
            subject: subject,
            cover_id: work.cover_id || null,
            first_publish_year: work.first_publish_year || null,
            source: 'openlibrary',
            source_id: work.key,
          });
        }
        
        offset += limit;
        await delay(500);
      } catch (error) {
        console.error(`Open Library error (${subject}): ${error.message}`);
        break;
      }
    }
  }
  
  return books;
}

async function scrapeGutenberg(options = {}) {
  const books = [];
  let page = 1;
  
  while (page <= 100) {
    try {
      const response = await fetchWithRetry(
        `https://gutendex.com/books/?page=${page}`
      );
      const data = await response.json();
      
      if (!data.results || data.results.length === 0) break;
      
      for (const book of data.results) {
        books.push({
          title: book.title,
          author: book.authors?.[0]?.name || '',
          subjects: (book.subjects || []).join(', '),
          languages: (book.languages || []).join(', '),
          download_count: book.download_count || 0,
          source: 'gutenberg',
          source_id: book.id?.toString(),
        });
      }
      
      page++;
      await delay(500);
    } catch (error) {
      console.error(`Gutenberg error: ${error.message}`);
      break;
    }
  }
  
  return books;
}

// =============================================================================
// TRANSFORMERS
// =============================================================================

function mapSpiritCategory(raw) {
  const cat = (raw || '').toLowerCase();
  if (cat.includes('whisk')) return 'bourbon';
  if (cat.includes('vodka')) return 'vodka';
  if (cat.includes('gin')) return 'gin';
  if (cat.includes('rum')) return 'rum';
  if (cat.includes('tequila')) return 'tequila';
  if (cat.includes('brandy') || cat.includes('cognac')) return 'other';
  if (cat.includes('liqueur')) return 'other';
  if (cat.includes('wine')) return 'wine';
  if (cat.includes('beer')) return 'beer';
  return 'other';
}

function transformSpirit(item) {
  const categoryMap = {
    bourbon: 'bourbon', whiskey: 'bourbon', whisky: 'bourbon',
    scotch: 'bourbon', rye: 'bourbon',
    vodka: 'vodka', gin: 'gin', rum: 'rum',
    tequila: 'tequila', mezcal: 'tequila',
    wine: 'wine', beer: 'beer', other: 'other'
  };
  
  return {
    name: (item.name || '').slice(0, 255),
    brand: (item.brand || '').slice(0, 255),
    category: categoryMap[(item.category || '').toLowerCase()] || 'other',
    size: (item.size || '').slice(0, 50),
    abv: typeof item.abv === 'number' ? item.abv : null,
    description: (item.description || '').slice(0, 2000),
    image_url: item.image_url || null,
    country: (item.country || '').slice(0, 100),
    region: (item.region || '').slice(0, 100),
    distillery: (item.distillery || item.brand || '').slice(0, 255),
    source: (item.source || 'scraper').slice(0, 100),
  };
}

function transformCard(item) {
  return {
    name: (item.name || '').slice(0, 255),
    set_name: (item.set_name || '').slice(0, 255),
    set_id: (item.set_id || '').slice(0, 50),
    number: (item.number || '').slice(0, 50),
    rarity: (item.rarity || '').slice(0, 50),
    image_url: item.image_url || null,
    price_market: typeof item.price_market === 'number' ? item.price_market : null,
    source: (item.source || 'scraper').slice(0, 100),
  };
}

function transformBook(item) {
  return {
    title: (item.title || '').slice(0, 500),
    author: (item.author || '').slice(0, 255),
    subject: (item.subject || item.subjects || '').slice(0, 255),
    cover_id: item.cover_id || null,
    first_publish_year: item.first_publish_year || null,
    source: (item.source || 'scraper').slice(0, 100),
    source_id: (item.source_id || '').slice(0, 100),
  };
}

// =============================================================================
// SUPABASE UPLOADER
// =============================================================================

async function uploadToSupabase(items, table, transform) {
  if (!CONFIG.supabase.serviceKey) {
    console.log('No Supabase key - skipping upload');
    return { uploaded: 0, errors: 0 };
  }
  
  const headers = {
    'apikey': CONFIG.supabase.serviceKey,
    'Authorization': `Bearer ${CONFIG.supabase.serviceKey}`,
    'Content-Type': 'application/json',
    'Prefer': 'resolution=ignore-duplicates,return=minimal',
  };
  
  let uploaded = 0;
  let errors = 0;
  const batchSize = 50;
  
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    const records = batch.map(transform).filter(r => r.name || r.title);
    
    if (records.length === 0) continue;
    
    try {
      const response = await fetch(`${CONFIG.supabase.url}/rest/v1/${table}`, {
        method: 'POST',
        headers,
        body: JSON.stringify(records),
      });
      
      if (response.ok) {
        uploaded += records.length;
      } else {
        errors += records.length;
        if (i === 0) {
          const text = await response.text();
          console.error(`Upload error: ${response.status} - ${text.slice(0, 200)}`);
        }
      }
    } catch (error) {
      errors += records.length;
    }
    
    await delay(50);
  }
  
  return { uploaded, errors };
}

// =============================================================================
// API HANDLER
// =============================================================================

export default async function handler(req, res) {
  // Optional auth
  if (CONFIG.scraperSecret) {
    const auth = req.headers.authorization;
    if (!auth || auth !== `Bearer ${CONFIG.scraperSecret}`) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
  }
  
  const { type = 'spirits', source = 'all', skip_upload = 'false' } = req.query;
  const skipUpload = skip_upload === 'true';
  
  // Validate type
  const scraperConfig = SCRAPERS[type];
  if (!scraperConfig) {
    return res.status(400).json({
      error: `Unknown type: ${type}`,
      available: Object.keys(SCRAPERS),
    });
  }
  
  const startTime = Date.now();
  let allItems = [];
  const results = {};
  
  try {
    // Determine which sources to run
    const sourcesToRun = source === 'all'
      ? Object.keys(scraperConfig.sources)
      : [source];
    
    // Run scrapers
    for (const srcKey of sourcesToRun) {
      const srcConfig = scraperConfig.sources[srcKey];
      if (!srcConfig) {
        results[srcKey] = { error: 'Unknown source' };
        continue;
      }
      
      console.log(`Running ${srcConfig.name}...`);
      
      try {
        const items = await srcConfig.fn({ limit: 10000 });
        allItems = allItems.concat(items);
        results[srcKey] = { count: items.length };
      } catch (error) {
        results[srcKey] = { error: error.message };
      }
    }
    
    // Upload
    let uploadStats = { uploaded: 0, errors: 0 };
    if (!skipUpload && allItems.length > 0) {
      uploadStats = await uploadToSupabase(
        allItems,
        scraperConfig.table,
        scraperConfig.transform
      );
    }
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    
    return res.status(200).json({
      success: true,
      type,
      source,
      results,
      total_scraped: allItems.length,
      uploaded: uploadStats.uploaded,
      errors: uploadStats.errors,
      duration_seconds: parseFloat(duration),
      timestamp: new Date().toISOString(),
    });
    
  } catch (error) {
    console.error('Scraper error:', error);
    return res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

export const config = {
  maxDuration: 300, // 5 minutes
};
