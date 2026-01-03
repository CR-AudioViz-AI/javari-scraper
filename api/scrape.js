// Javari Universal Data Scraper
// ==============================
// A reusable scraper framework for any data type
// 
// Endpoints:
//   GET /api/scrape?type=spirits&source=all
//   GET /api/scrape?type=spirits&source=ttb_cola  <-- NEW! 500K+ products
//   GET /api/scrape?type=cards&source=pokemon
//   GET /api/scrape?type=books&source=openlibrary
//
// Author: Javari AI for CR AudioViz AI LLC
// Created: 2026-01-02
// Updated: 2026-01-02 - Added TTB COLA Registry (500K+ US alcohol labels)

// =============================================================================
// CONFIGURATION
// =============================================================================

const CONFIG = {
  supabase: {
    url: process.env.SUPABASE_URL || 'https://kteobfyferrukqeolofj.supabase.co',
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
        await delay(5000 * (i + 1));
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
// SCRAPER REGISTRY
// =============================================================================

const SCRAPERS = {
  spirits: {
    table: 'bv_spirits',
    sources: {
      ttb_cola: {
        name: 'TTB COLA Registry',
        fn: scrapeTTBCOLA,
        schedule: 'daily',
        estimated: 500000, // 500K+ approved US alcohol labels
        description: 'US Government alcohol label database (public domain)',
      },
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
        estimated: 1000,
        requiresAuth: true,
      },
    },
    transform: transformSpirit,
  },
  
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
// TTB COLA REGISTRY SCRAPER (NEW - 500K+ products)
// =============================================================================

async function scrapeTTBCOLA(options = {}) {
  const results = [];
  const limit = options.limit || 5000; // Default batch size
  
  // TTB uses a web form, we'll scrape by iterating through TTB IDs
  // TTB ID format: YYXXXXXXXX (2-digit year + 8 digit sequence)
  // Recent years: 24, 25, 26 for 2024, 2025, 2026
  
  console.log(`[TTB] Starting COLA scrape (limit: ${limit})`);
  
  // Strategy: Iterate through recent TTB IDs
  const currentYear = new Date().getFullYear() % 100; // e.g., 26 for 2026
  const years = [currentYear, currentYear - 1, currentYear - 2]; // Last 3 years
  
  let totalScraped = 0;
  
  for (const year of years) {
    if (totalScraped >= limit) break;
    
    const yearPrefix = String(year).padStart(2, '0');
    console.log(`[TTB] Scraping year 20${yearPrefix}...`);
    
    // Each year has ~50,000-100,000 COLAs
    // We'll sample from the sequence
    const baseId = parseInt(`${yearPrefix}00000001`);
    const step = 100; // Sample every 100th ID
    
    for (let seq = 0; seq < 100000 && totalScraped < limit; seq += step) {
      const ttbId = String(baseId + seq).padStart(10, '0');
      
      try {
        const record = await fetchTTBColaDetails(ttbId);
        if (record && record.brand_name) {
          results.push(record);
          totalScraped++;
          
          if (totalScraped % 100 === 0) {
            console.log(`[TTB] Scraped ${totalScraped} records...`);
          }
        }
        
        // Rate limiting - be respectful to government servers
        await delay(200);
        
      } catch (error) {
        // Skip invalid IDs
        continue;
      }
    }
  }
  
  console.log(`[TTB] Total scraped: ${results.length}`);
  return results;
}

async function fetchTTBColaDetails(ttbId) {
  const url = `https://www.ttbonline.gov/colasonline/publicSearchColasBasic.do?action=publicDisplaySearchBasic&ttbid=${ttbId}`;
  
  try {
    const response = await fetch(url, {
      headers: {
        'User-Agent': 'JavariSpirits/1.0 (CR AudioViz AI LLC - spirits database)',
        'Accept': 'text/html',
      },
    });
    
    if (!response.ok) return null;
    
    const html = await response.text();
    
    // Check if valid COLA found
    if (html.includes('No records found') || html.includes('Invalid TTB ID')) {
      return null;
    }
    
    // Extract fields from HTML
    const extract = (pattern) => {
      const match = html.match(pattern);
      return match ? match[1].trim() : null;
    };
    
    // TTB page structure has labeled table cells
    const brandName = extract(/Brand Name[^<]*<\/td>\s*<td[^>]*>([^<]+)/i);
    const fancifulName = extract(/Fanciful Name[^<]*<\/td>\s*<td[^>]*>([^<]+)/i);
    const classType = extract(/Class(?:\/| and )Type[^<]*<\/td>\s*<td[^>]*>([^<]+)/i);
    const origin = extract(/Origin[^<]*<\/td>\s*<td[^>]*>([^<]+)/i);
    const alcoholContent = extract(/Alcohol Content[^<]*<\/td>\s*<td[^>]*>([\d.]+)/i);
    const dateApproved = extract(/Date (?:Approved|Completed)[^<]*<\/td>\s*<td[^>]*>([^<]+)/i);
    const permitNumber = extract(/(?:Basic |)Permit[^<]*<\/td>\s*<td[^>]*>([^<]+)/i);
    
    // Determine category
    let category = 'spirits';
    if (classType) {
      const lc = classType.toLowerCase();
      if (lc.includes('wine')) category = 'wine';
      else if (lc.includes('beer') || lc.includes('malt') || lc.includes('ale') || lc.includes('lager')) category = 'beer';
    }
    
    return {
      ttb_id: ttbId,
      brand_name: brandName || fancifulName,
      fanciful_name: fancifulName,
      class_type: classType,
      origin: origin,
      alcohol_content: alcoholContent,
      permit_number: permitNumber,
      date_approved: dateApproved,
      category: category,
      source: 'ttb_cola',
    };
    
  } catch (error) {
    return null;
  }
}

// =============================================================================
// OPEN FOOD FACTS SCRAPER
// =============================================================================

async function scrapeOpenFoodFacts(options = {}) {
  const results = [];
  const categories = [
    'en:alcoholic-beverages',
    'en:wines',
    'en:beers', 
    'en:spirits',
    'en:whiskeys',
    'en:vodkas',
    'en:rums',
    'en:tequilas',
    'en:gins',
    'en:brandies',
    'en:liqueurs',
  ];
  
  const limit = options.limit || 20000;
  let total = 0;
  
  for (const category of categories) {
    if (total >= limit) break;
    
    let page = 1;
    const pageSize = 100;
    
    while (total < limit) {
      try {
        const url = `https://world.openfoodfacts.org/category/${category}.json?page=${page}&page_size=${pageSize}`;
        const response = await fetchWithRetry(url);
        const data = await response.json();
        
        if (!data.products || data.products.length === 0) break;
        
        for (const product of data.products) {
          if (total >= limit) break;
          
          results.push({
            name: product.product_name || product.product_name_en,
            brand: product.brands,
            category: mapOFFCategory(product.categories_tags),
            description: product.generic_name,
            image_url: product.image_url,
            barcode: product.code,
            origin: product.origins,
            alcohol_content: parseFloat(product.alcohol_100g) || null,
            source: 'openfoodfacts',
            external_ids: { off_id: product.code },
          });
          total++;
        }
        
        page++;
        await delay(300);
        
      } catch (error) {
        console.error(`[OFF] Error on ${category} page ${page}:`, error.message);
        break;
      }
    }
  }
  
  return results;
}

function mapOFFCategory(tags) {
  if (!tags || !Array.isArray(tags)) return 'spirits';
  const tagStr = tags.join(',').toLowerCase();
  
  if (tagStr.includes('whiskey') || tagStr.includes('whisky') || tagStr.includes('bourbon')) return 'bourbon';
  if (tagStr.includes('vodka')) return 'vodka';
  if (tagStr.includes('rum')) return 'rum';
  if (tagStr.includes('tequila') || tagStr.includes('mezcal')) return 'tequila';
  if (tagStr.includes('gin')) return 'gin';
  if (tagStr.includes('brandy') || tagStr.includes('cognac')) return 'brandy';
  if (tagStr.includes('wine')) return 'wine';
  if (tagStr.includes('beer') || tagStr.includes('ale') || tagStr.includes('lager')) return 'beer';
  if (tagStr.includes('liqueur')) return 'other';
  return 'spirits';
}

// =============================================================================
// OPEN BREWERY DB SCRAPER
// =============================================================================

async function scrapeOpenBreweryDB(options = {}) {
  const results = [];
  const limit = options.limit || 9000;
  let page = 1;
  const perPage = 200;
  
  while (results.length < limit) {
    try {
      const url = `https://api.openbrewerydb.org/v1/breweries?page=${page}&per_page=${perPage}`;
      const response = await fetchWithRetry(url);
      const breweries = await response.json();
      
      if (!breweries || breweries.length === 0) break;
      
      for (const brewery of breweries) {
        results.push({
          name: brewery.name,
          brand: brewery.name,
          category: 'beer',
          subcategory: brewery.brewery_type,
          country: brewery.country || 'United States',
          region: brewery.state,
          description: `${brewery.brewery_type} brewery in ${brewery.city}, ${brewery.state}`,
          source: 'openbrewerydb',
          external_ids: { brewery_id: brewery.id },
        });
      }
      
      page++;
      await delay(200);
      
    } catch (error) {
      console.error(`[Brewery] Error on page ${page}:`, error.message);
      break;
    }
  }
  
  return results.slice(0, limit);
}

// =============================================================================
// PUNK API SCRAPER (BrewDog)
// =============================================================================

async function scrapePunkAPI(options = {}) {
  const results = [];
  const limit = options.limit || 500;
  let page = 1;
  const perPage = 80;
  
  while (results.length < limit) {
    try {
      const url = `https://api.punkapi.com/v2/beers?page=${page}&per_page=${perPage}`;
      const response = await fetchWithRetry(url);
      const beers = await response.json();
      
      if (!beers || beers.length === 0) break;
      
      for (const beer of beers) {
        results.push({
          name: beer.name,
          brand: 'BrewDog',
          category: 'beer',
          subcategory: beer.tagline,
          abv: beer.abv,
          description: beer.description,
          image_url: beer.image_url,
          tasting_notes: beer.brewers_tips,
          source: 'punkapi',
          external_ids: { punkapi_id: beer.id },
        });
      }
      
      page++;
      await delay(200);
      
    } catch (error) {
      console.error(`[PunkAPI] Error on page ${page}:`, error.message);
      break;
    }
  }
  
  return results.slice(0, limit);
}

// =============================================================================
// COCKTAIL DB SCRAPER
// =============================================================================

async function scrapeCocktailDB(options = {}) {
  const results = [];
  
  // CocktailDB free tier: search by first letter
  const letters = 'abcdefghijklmnopqrstuvwxyz'.split('');
  
  for (const letter of letters) {
    try {
      const url = `https://www.thecocktaildb.com/api/json/v1/1/search.php?f=${letter}`;
      const response = await fetchWithRetry(url);
      const data = await response.json();
      
      if (data.drinks) {
        for (const drink of data.drinks) {
          // Get ingredients
          const ingredients = [];
          for (let i = 1; i <= 15; i++) {
            const ing = drink[`strIngredient${i}`];
            if (ing) ingredients.push(ing);
          }
          
          results.push({
            name: drink.strDrink,
            brand: null,
            category: 'cocktail',
            subcategory: drink.strCategory,
            description: drink.strInstructions,
            image_url: drink.strDrinkThumb,
            tasting_notes: ingredients.join(', '),
            source: 'cocktaildb',
            external_ids: { cocktaildb_id: drink.idDrink },
          });
        }
      }
      
      await delay(300);
      
    } catch (error) {
      console.error(`[CocktailDB] Error on letter ${letter}:`, error.message);
    }
  }
  
  return results;
}

// =============================================================================
// UNTAPPD SCRAPER (requires API key)
// =============================================================================

async function scrapeUntappd(options = {}) {
  if (!CONFIG.untappd.clientId || !CONFIG.untappd.clientSecret) {
    console.log('[Untappd] Skipping - no credentials configured');
    return [];
  }
  
  // Untappd has strict rate limits, implement carefully
  const results = [];
  // TODO: Implement when API key is approved
  
  return results;
}

// =============================================================================
// POKEMON TCG SCRAPER
// =============================================================================

async function scrapePokemonTCG(options = {}) {
  const results = [];
  const limit = options.limit || 15000;
  let page = 1;
  const pageSize = 250;
  
  while (results.length < limit) {
    try {
      const url = `https://api.pokemontcg.io/v2/cards?page=${page}&pageSize=${pageSize}`;
      const response = await fetchWithRetry(url);
      const data = await response.json();
      
      if (!data.data || data.data.length === 0) break;
      
      for (const card of data.data) {
        results.push({
          name: card.name,
          set: card.set?.name,
          rarity: card.rarity,
          image_url: card.images?.large || card.images?.small,
          source: 'pokemontcg',
          external_ids: { pokemon_id: card.id },
        });
      }
      
      page++;
      await delay(500);
      
    } catch (error) {
      console.error(`[Pokemon] Error on page ${page}:`, error.message);
      break;
    }
  }
  
  return results.slice(0, limit);
}

// =============================================================================
// SCRYFALL SCRAPER (MTG)
// =============================================================================

async function scrapeScryfall(options = {}) {
  const results = [];
  const limit = options.limit || 80000;
  let url = 'https://api.scryfall.com/cards/search?q=*';
  
  while (results.length < limit && url) {
    try {
      const response = await fetchWithRetry(url);
      const data = await response.json();
      
      if (data.data) {
        for (const card of data.data) {
          results.push({
            name: card.name,
            set: card.set_name,
            rarity: card.rarity,
            image_url: card.image_uris?.normal || card.image_uris?.small,
            source: 'scryfall',
            external_ids: { scryfall_id: card.id },
          });
        }
      }
      
      url = data.has_more ? data.next_page : null;
      await delay(100);
      
    } catch (error) {
      console.error(`[Scryfall] Error:`, error.message);
      break;
    }
  }
  
  return results.slice(0, limit);
}

// =============================================================================
// OPEN LIBRARY SCRAPER
// =============================================================================

async function scrapeOpenLibrary(options = {}) {
  const results = [];
  const subjects = ['fiction', 'science', 'history', 'biography', 'fantasy', 'mystery'];
  const limit = options.limit || 50000;
  
  for (const subject of subjects) {
    if (results.length >= limit) break;
    
    let offset = 0;
    const batchSize = 100;
    
    while (results.length < limit) {
      try {
        const url = `https://openlibrary.org/subjects/${subject}.json?limit=${batchSize}&offset=${offset}`;
        const response = await fetchWithRetry(url);
        const data = await response.json();
        
        if (!data.works || data.works.length === 0) break;
        
        for (const work of data.works) {
          results.push({
            name: work.title,
            author: work.authors?.[0]?.name,
            subject: subject,
            cover_url: work.cover_id ? `https://covers.openlibrary.org/b/id/${work.cover_id}-M.jpg` : null,
            source: 'openlibrary',
            external_ids: { ol_key: work.key },
          });
        }
        
        offset += batchSize;
        await delay(500);
        
      } catch (error) {
        console.error(`[OpenLibrary] Error on ${subject}:`, error.message);
        break;
      }
    }
  }
  
  return results.slice(0, limit);
}

// =============================================================================
// PROJECT GUTENBERG SCRAPER
// =============================================================================

async function scrapeGutenberg(options = {}) {
  // Gutenberg has RSS feeds and bulk data
  // Using their simple catalog endpoint
  const results = [];
  const limit = options.limit || 1000;
  
  try {
    // Use Gutenberg search API
    const url = 'https://gutendex.com/books/';
    let nextUrl = url;
    
    while (results.length < limit && nextUrl) {
      const response = await fetchWithRetry(nextUrl);
      const data = await response.json();
      
      if (data.results) {
        for (const book of data.results) {
          results.push({
            name: book.title,
            author: book.authors?.[0]?.name,
            subject: book.subjects?.join(', '),
            download_url: book.formats?.['text/plain; charset=utf-8'] || book.formats?.['text/plain'],
            source: 'gutenberg',
            external_ids: { gutenberg_id: book.id },
          });
        }
      }
      
      nextUrl = data.next;
      await delay(300);
    }
    
  } catch (error) {
    console.error(`[Gutenberg] Error:`, error.message);
  }
  
  return results.slice(0, limit);
}

// =============================================================================
// TRANSFORM FUNCTIONS
// =============================================================================

function transformSpirit(record) {
  // Map to bv_spirits table schema
  return {
    name: (record.name || record.brand_name || 'Unknown').substring(0, 255),
    brand: record.brand?.substring(0, 255) || null,
    category: mapCategory(record.category),
    subcategory: record.subcategory?.substring(0, 100) || record.class_type?.substring(0, 100) || null,
    country: record.country?.substring(0, 100) || record.origin?.substring(0, 100) || null,
    region: record.region?.substring(0, 100) || null,
    abv: typeof record.abv === 'number' ? record.abv : (parseFloat(record.alcohol_content) || null),
    description: record.description?.substring(0, 2000) || null,
    image_url: record.image_url?.substring(0, 500) || null,
    tasting_notes: record.tasting_notes?.substring(0, 1000) || null,
    external_ids: record.external_ids ? JSON.stringify(record.external_ids) : 
                  record.ttb_id ? JSON.stringify({ ttb_id: record.ttb_id }) : null,
  };
}

function mapCategory(cat) {
  if (!cat) return 'spirits';
  const c = String(cat).toLowerCase();
  
  if (c.includes('bourbon') || c.includes('whiskey') || c.includes('whisky')) return 'bourbon';
  if (c.includes('vodka')) return 'vodka';
  if (c.includes('rum')) return 'rum';
  if (c.includes('tequila') || c.includes('mezcal')) return 'tequila';
  if (c.includes('gin')) return 'gin';
  if (c.includes('brandy') || c.includes('cognac')) return 'brandy';
  if (c.includes('wine')) return 'wine';
  if (c.includes('beer') || c.includes('ale') || c.includes('lager') || c.includes('malt')) return 'beer';
  if (c.includes('cocktail')) return 'cocktail';
  if (c.includes('spirits')) return 'spirits';
  return 'other';
}

function transformCard(record) {
  return {
    name: record.name,
    set_name: record.set,
    rarity: record.rarity,
    image_url: record.image_url,
    source: record.source,
    external_ids: record.external_ids ? JSON.stringify(record.external_ids) : null,
  };
}

function transformBook(record) {
  return {
    title: record.name,
    author: record.author,
    subject: record.subject,
    cover_url: record.cover_url,
    download_url: record.download_url,
    source: record.source,
    external_ids: record.external_ids ? JSON.stringify(record.external_ids) : null,
  };
}

// =============================================================================
// SUPABASE UPLOAD
// =============================================================================

async function uploadToSupabase(records, table) {
  if (!CONFIG.supabase.serviceKey) {
    throw new Error('SUPABASE_SERVICE_KEY not configured');
  }
  
  const batchSize = 50;
  let uploaded = 0;
  let errors = 0;
  
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    
    try {
      const response = await fetch(`${CONFIG.supabase.url}/rest/v1/${table}`, {
        method: 'POST',
        headers: {
          'apikey': CONFIG.supabase.serviceKey,
          'Authorization': `Bearer ${CONFIG.supabase.serviceKey}`,
          'Content-Type': 'application/json',
          'Prefer': 'resolution=ignore-duplicates,return=minimal',
        },
        body: JSON.stringify(batch),
      });
      
      if (response.ok) {
        uploaded += batch.length;
      } else {
        const errorText = await response.text();
        console.error(`Upload error (batch ${i}): ${response.status} - ${errorText}`);
        errors += batch.length;
      }
      
    } catch (error) {
      console.error(`Upload error (batch ${i}):`, error.message);
      errors += batch.length;
    }
    
    await delay(50);
  }
  
  return { uploaded, errors };
}

// =============================================================================
// MAIN HANDLER
// =============================================================================

export default async function handler(req, res) {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  
  const { type, source, skip_upload, limit } = req.query;
  
  if (!type || !SCRAPERS[type]) {
    return res.status(400).json({
      error: 'Invalid type',
      validTypes: Object.keys(SCRAPERS),
    });
  }
  
  const scraper = SCRAPERS[type];
  const sources = source === 'all' 
    ? Object.keys(scraper.sources) 
    : [source].filter(s => scraper.sources[s]);
  
  if (sources.length === 0) {
    return res.status(400).json({
      error: 'Invalid source',
      validSources: Object.keys(scraper.sources),
    });
  }
  
  const results = {
    type,
    sources: {},
    totalScraped: 0,
    totalUploaded: 0,
    totalErrors: 0,
    timestamp: new Date().toISOString(),
  };
  
  for (const sourceName of sources) {
    const sourceConfig = scraper.sources[sourceName];
    
    if (sourceConfig.requiresAuth && !CONFIG.untappd.clientId) {
      results.sources[sourceName] = { skipped: true, reason: 'Requires authentication' };
      continue;
    }
    
    try {
      console.log(`[${type}] Scraping ${sourceConfig.name}...`);
      
      const options = { limit: limit ? parseInt(limit) : undefined };
      const records = await sourceConfig.fn(options);
      const transformed = records.map(scraper.transform);
      
      results.sources[sourceName] = {
        scraped: transformed.length,
        uploaded: 0,
        errors: 0,
      };
      
      results.totalScraped += transformed.length;
      
      if (skip_upload !== 'true' && transformed.length > 0) {
        const uploadResult = await uploadToSupabase(transformed, scraper.table);
        results.sources[sourceName].uploaded = uploadResult.uploaded;
        results.sources[sourceName].errors = uploadResult.errors;
        results.totalUploaded += uploadResult.uploaded;
        results.totalErrors += uploadResult.errors;
      }
      
    } catch (error) {
      console.error(`[${type}] Error scraping ${sourceName}:`, error);
      results.sources[sourceName] = {
        error: error.message,
      };
    }
  }
  
  return res.status(200).json(results);
}
