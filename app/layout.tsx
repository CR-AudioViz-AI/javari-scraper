// app/layout.tsx
import type { Metadata } from 'next'
export const dynamic = 'force-dynamic'
export const metadata: Metadata = {
  title: 'Javari Scraper | CR AudioViz AI — EIN 39-3646201',
  description: 'Javari data collection and scraping tools',
}
export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body style={{ margin: 0, padding: 0, background: '#07080f' }}>
        {children}
      </body>
    </html>
  )
}
