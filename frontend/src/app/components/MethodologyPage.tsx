const methodologyBlocks = [
  {
    n: '01',
    title: 'Data sources',
    body: 'The prototype combines public review text, structured apartment details, and route data so students can compare both lived experience and practical fit.',
    rows: [
      ['Google Maps reviews', '911 collected'],
      ['Reddit housing mentions', '960 resolved'],
      ['VADER Reddit rows', '283 scored'],
      ['Pricing records', '137 rows'],
      ['Commute matrix', '121 routes'],
    ],
  },
  {
    n: '02',
    title: 'Apartment matching',
    body: 'Every processed dataset should join on the same apartment_id, which prevents source-specific names from splitting one apartment into multiple records.',
    rows: [
      ['HERE Champaign', 'here'],
      ['The Tower at Third / T3', 'tower_at_third'],
      ['Seven07 / 707', 'seven07'],
      ['75 Armory', 'armory_75'],
      ['Illini Manor Apartments', 'illini_manor'],
    ],
  },
  {
    n: '03',
    title: 'Sentiment analysis',
    body: 'Review text and Reddit comments are scored with VADER. The compound score gives us a quick, explainable first pass at whether a comment reads positive, neutral, or negative.',
    rows: [
      ['compound >= 0.05', 'positive'],
      ['-0.05 < compound < 0.05', 'neutral'],
      ['compound <= -0.05', 'negative'],
    ],
  },
  {
    n: '04',
    title: 'Theme tagging',
    body: 'Sentiment alone is not enough. We also tag what each comment is about, so a student can see whether complaints are about maintenance, noise, management, fees, or location.',
    rows: [
      ['maintenance', 'repair, leak, broken, work order'],
      ['fees', 'fee, charge, deposit, refund'],
      ['management', 'manager, office, staff, landlord'],
      ['noise', 'loud, party, thin walls'],
      ['location', 'walk, campus, bus, convenient'],
    ],
  },
  {
    n: '05',
    title: 'Score formula',
    body: 'The demo score is weighted toward human review evidence, then adjusted with structured apartment data and commute practicality.',
    rows: [
      ['Google review sentiment', '35%'],
      ['Reddit sentiment', '20%'],
      ['Rent value', '20%'],
      ['Amenities and utilities', '15%'],
      ['Commute convenience', '10%'],
    ],
  },
  {
    n: '06',
    title: 'Confidence and limits',
    body: 'Each apartment should show its evidence counts next to the score. A score backed by 100 Google reviews is more reliable than one backed by only a few comments.',
    rows: [
      ['Reviews can be biased', 'both positive and negative'],
      ['Reddit can overrepresent problems', 'students post when frustrated'],
      ['Google reviews can be low-detail', 'stars may not explain why'],
      ['VADER misses context', 'sarcasm and local slang are hard'],
      ['Rent data changes quickly', 'prices need refreshes'],
    ],
  },
];

export function MethodologyPage() {
  return (
    <>
      <div className="mx-auto px-8" style={{ maxWidth: '1280px' }}>
        <section className="py-10 pb-0">
          <div
            className="mb-3.5"
            style={{
              fontFamily: 'var(--mono)',
              fontSize: '11px',
              letterSpacing: '0.1em',
              textTransform: 'uppercase',
              color: 'var(--orange)',
            }}
          >
            § Methodology
          </div>
          <h1
            className="m-0"
            style={{
              fontFamily: 'var(--serif)',
              fontWeight: 500,
              fontSize: 'clamp(48px, 7.2vw, 92px)',
              lineHeight: 0.96,
              letterSpacing: '-0.025em',
              maxWidth: '980px',
            }}
          >
            How each apartment score is built.
          </h1>
          <p
            style={{
              fontFamily: 'var(--serif)',
              fontSize: '20px',
              lineHeight: 1.5,
              color: 'var(--ink-2)',
              maxWidth: '760px',
              margin: '28px 0 0',
            }}
          >
            This page explains where the data comes from, how messy apartment names are joined
            across sources, how VADER sentiment is interpreted, and what the current prototype can
            and cannot claim.
          </p>
        </section>

        <section className="pt-12 pb-0">
          <div
            className="grid gap-0"
            style={{
              borderTop: '1px solid var(--ink)',
              borderLeft: '1px solid var(--ink)',
              gridTemplateColumns: 'repeat(2, 1fr)',
            }}
          >
            {methodologyBlocks.map((block) => (
              <div
                key={block.n}
                className="px-6 py-6"
                style={{
                  borderRight: '1px solid var(--ink)',
                  borderBottom: '1px solid var(--ink)',
                  minHeight: '310px',
                }}
              >
                <div
                  style={{
                    fontFamily: 'var(--mono)',
                    fontSize: '11px',
                    letterSpacing: '0.08em',
                    color: 'var(--orange)',
                    marginBottom: '14px',
                  }}
                >
                  {block.n}
                </div>
                <h2
                  style={{
                    fontFamily: 'var(--serif)',
                    fontWeight: 500,
                    fontSize: '28px',
                    letterSpacing: '-0.01em',
                    lineHeight: 1.1,
                    margin: '0 0 14px',
                  }}
                >
                  {block.title}
                </h2>
                <p
                  style={{
                    fontSize: '14.5px',
                    lineHeight: 1.6,
                    color: 'var(--ink-2)',
                    margin: '0 0 18px',
                  }}
                >
                  {block.body}
                </p>
                <ul
                  className="list-none p-0"
                  style={{
                    fontFamily: 'var(--mono)',
                    fontSize: '11px',
                    color: 'var(--ink-3)',
                    letterSpacing: '0.02em',
                    textTransform: 'uppercase',
                    margin: 0,
                  }}
                >
                  {block.rows.map(([label, value]) => (
                    <li
                      key={`${block.n}-${label}`}
                      className="flex justify-between"
                      style={{
                        gap: '18px',
                        padding: '8px 0',
                        borderTop: '1px solid var(--rule)',
                      }}
                    >
                      <span>{label}</span>
                      <b style={{ color: 'var(--ink)', fontWeight: 500, textAlign: 'right' }}>
                        {value}
                      </b>
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </div>
        </section>
      </div>
    </>
  );
}
