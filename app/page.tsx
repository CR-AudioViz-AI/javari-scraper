// app/page.tsx — Javari Scraper
// Javari data collection and scraping tools
// CR AudioViz AI, LLC · EIN 39-3646201
export const dynamic = 'force-dynamic'
export default function Home() {
  return (
    <div style={{ background: '#07080f', minHeight: '100vh', color: 'white',
                  fontFamily: 'Inter, system-ui, sans-serif' }}>
      <nav style={{ position: 'fixed', top: 0, left: 0, right: 0, zIndex: 100,
                    background: 'rgba(7,7,16,0.97)', borderBottom: '1px solid rgba(255,255,255,0.06)',
                    height: 58, display: 'flex', alignItems: 'center',
                    justifyContent: 'space-between', padding: '0 24px' }}>
        <span style={{ fontWeight: 800, fontSize: 15, color: '#6366f1' }}>Javari Scraper</span>
        <div style={{ display: 'flex', gap: 8 }}>
          <a href="https://craudiovizai.com/pricing" style={{ color: '#6b7280', fontSize: 13,
                textDecoration: 'none', padding: '5px 10px' }}>Pricing</a>
          <a href="https://javariai.com" style={{ color: '#6b7280', fontSize: 13,
                textDecoration: 'none', padding: '5px 10px' }}>Javari AI</a>
          <a href="https://craudiovizai.com/auth/signup" style={{ background: '#6366f1',
                color: 'white', borderRadius: 8, padding: '8px 18px',
                fontSize: 13, fontWeight: 700, textDecoration: 'none' }}>
            Get Started Free →
          </a>
        </div>
      </nav>
      <div style={{ height: 58 }} />
      <section style={{ textAlign: 'center', padding: '100px 24px 80px',
                        maxWidth: 720, margin: '0 auto' }}>
        <h1 style={{ fontSize: 'clamp(32px,5vw,56px)', fontWeight: 900,
                      margin: '0 0 20px', lineHeight: 1.1 }}>
          Javari Scraper
        </h1>
        <p style={{ fontSize: 18, color: '#9ca3af', margin: '0 auto 40px',
                     maxWidth: 520, lineHeight: 1.6 }}>
          Javari data collection and scraping tools
        </p>
        <div style={{ display: 'flex', gap: 12, justifyContent: 'center',
                       flexWrap: 'wrap' }}>
          <a href="https://craudiovizai.com/auth/signup" style={{
              background: 'linear-gradient(135deg,#6366f1,#8b5cf6)',
              color: 'white', borderRadius: 12, padding: '14px 32px',
              fontSize: 16, fontWeight: 700, textDecoration: 'none' }}>
            Start Free — No Card →
          </a>
          <a href="https://javariai.com/javari" style={{
              background: 'rgba(255,255,255,0.08)', color: 'white',
              border: '1px solid rgba(255,255,255,0.1)', borderRadius: 12,
              padding: '14px 24px', fontSize: 16, fontWeight: 700,
              textDecoration: 'none' }}>
            🤖 Ask Javari AI
          </a>
        </div>
      </section>
      <footer style={{ background: '#030308', borderTop: '1px solid rgba(255,255,255,0.04)',
                        padding: '24px', textAlign: 'center', color: '#374151',
                        fontSize: 12 }}>
        © 2026 CR AudioViz AI, LLC · EIN 39-3646201 · Fort Myers, Florida ·
        Your Story. Our Design. Everyone Connects. Everyone Wins.
      </footer>
    </div>
  )
}
