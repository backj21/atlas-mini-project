interface HomePageProps {
  onNavigateToMap: () => void;
}

export function HomePage({ onNavigateToMap }: HomePageProps) {
  return (
    <div className="mx-auto px-8" style={{ maxWidth: '1280px' }}>
      {/* Hero */}
      <section className="py-10 pb-7">
        <div className="flex items-center mb-5.5" style={{ gap: '12px' }}>
          <span
            className="px-2 py-0.5"
            style={{
              background: 'var(--ink)',
              color: 'var(--cream)',
              fontFamily: 'var(--mono)',
              fontSize: '11px',
              letterSpacing: '0.12em',
              textTransform: 'uppercase',
            }}
          >
            Field guide
          </span>
          <span
            style={{
              fontFamily: 'var(--mono)',
              fontSize: '11px',
              letterSpacing: '0.1em',
              textTransform: 'uppercase',
              color: 'var(--ink-3)',
            }}
          >
            A student-built research tool for UIUC housing
          </span>
        </div>

        <h1
          className="m-0 mb-2"
          style={{
            fontFamily: 'var(--serif)',
            fontWeight: 500,
            fontSize: 'clamp(48px, 7.2vw, 104px)',
            lineHeight: 0.96,
            letterSpacing: '-0.025em',
            maxWidth: '1100px',
          }}
        >
          Finally, an apartment site
          <br />
          that <em style={{ fontStyle: 'italic', color: 'var(--blue)' }}>isn't</em> trying to{' '}
          <span
            style={{
              textDecoration: 'line-through',
              textDecorationThickness: '3px',
              color: 'var(--ink-4)',
              fontStyle: 'normal',
            }}
          >
            rent
          </span>{' '}
          sell you one.
        </h1>

        <div
          className="grid gap-10 pt-7 mt-8"
          style={{
            gridTemplateColumns: '1fr 1fr 1fr',
            borderTop: '1px solid var(--ink)',
          }}
        >
          <div
            style={{
              fontFamily: 'var(--mono)',
              fontSize: '11px',
              letterSpacing: '0.06em',
              textTransform: 'uppercase',
              color: 'var(--ink-3)',
              lineHeight: 1.7,
            }}
          >
            <div>
              <b style={{ color: 'var(--ink)', fontWeight: 500 }}>REPORTED BY</b>
            </div>
            <div>The IlliniNest team</div>
            <div>UIUC student research project</div>
            <div style={{ marginTop: '14px' }}>
              <b style={{ color: 'var(--ink)', fontWeight: 500 }}>LAST UPDATE</b>
            </div>
            <div>Apr 30, 2026 · Prototype dataset</div>
            <div style={{ marginTop: '14px' }}>
              <b style={{ color: 'var(--ink)', fontWeight: 500 }}>COVERAGE</b>
            </div>
            <div>11 apartments · 12 scored records</div>
          </div>

          <p
            className="m-0"
            style={{
              fontFamily: 'var(--serif)',
              fontSize: '20px',
              lineHeight: 1.5,
              gridColumn: 'span 2',
              letterSpacing: '-0.005em',
            }}
          >
            <span
              style={{
                float: 'left',
                fontSize: '58px',
                lineHeight: 0.85,
                padding: '6px 8px 0 0',
                fontWeight: 600,
                color: 'var(--orange)',
              }}
            >
              W
            </span>
            e are building a focused UIUC housing dataset from Google Maps reviews, Reddit housing
            discussions, apartment listing data, and campus commute times. The first demo narrows
            the product to apartments where multiple sources overlap, so each score can point back
            to real student-facing evidence instead of marketing copy.
          </p>
        </div>

        {/* Stat Strip */}
        <div
          className="grid mt-12"
          style={{
            gridTemplateColumns: 'repeat(4, 1fr)',
            borderTop: '1px solid var(--ink)',
            borderBottom: '1px solid var(--ink)',
          }}
        >
          {[
            { num: '11', unit: 'apts', label: 'Apartment profiles with Google and commute data' },
            { num: '911', unit: 'reviews', label: 'Google Maps reviews collected for analysis' },
            { num: '960', unit: 'mentions', label: 'Reddit housing mentions resolved to complexes' },
            { num: '121', unit: 'routes', label: 'Apartment-to-campus commute rows processed' },
          ].map((stat, i, arr) => (
            <div
              key={i}
              className="px-6 py-6"
              style={{
                borderRight: i < arr.length - 1 ? '1px solid var(--rule)' : '0',
              }}
            >
              <div
                style={{
                  fontFamily: 'var(--serif)',
                  fontSize: '56px',
                  lineHeight: 1,
                  letterSpacing: '-0.03em',
                  fontWeight: 500,
                }}
              >
                {stat.num}
                <span
                  style={{
                    fontFamily: 'var(--mono)',
                    fontSize: '13px',
                    letterSpacing: '0.04em',
                    color: 'var(--ink-3)',
                    marginLeft: '4px',
                    verticalAlign: '4px',
                  }}
                >
                  {stat.unit}
                </span>
              </div>
              <div
                style={{
                  fontFamily: 'var(--mono)',
                  fontSize: '10.5px',
                  letterSpacing: '0.08em',
                  textTransform: 'uppercase',
                  color: 'var(--ink-3)',
                  marginTop: '10px',
                  lineHeight: 1.5,
                }}
              >
                {stat.label}
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* Supported Apartments */}
      <section className="pt-18 pb-0" style={{ paddingTop: '72px' }}>
        <div className="grid gap-10 items-end mb-8" style={{ gridTemplateColumns: '1fr 2fr' }}>
          <div>
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
              § 01 — Current coverage
            </div>
            <h2
              className="m-0"
              style={{
                fontFamily: 'var(--serif)',
                fontWeight: 500,
                fontSize: '44px',
                lineHeight: 1.05,
                letterSpacing: '-0.02em',
              }}
            >
              Apartments in this demo
            </h2>
          </div>
          <div
            className="ml-auto"
            style={{
              fontFamily: 'var(--serif)',
              fontSize: '18px',
              lineHeight: 1.55,
              color: 'var(--ink-2)',
              maxWidth: '560px',
            }}
          >
            These are the apartments currently covered by the prototype dataset. We are prioritizing
            buildings where Google reviews, commute data, pricing, amenities, or apartment-site
            reviews can be connected through a shared apartment ID.
          </div>
        </div>

        <div
          className="grid"
          style={{
            gridTemplateColumns: 'repeat(4, 1fr)',
            borderTop: '1px solid var(--ink)',
            borderLeft: '1px solid var(--ink)',
          }}
        >
          {[
            'HERE Champaign',
            'Hub on Campus Champaign',
            'The Dean Campustown',
            'ICON Apartments',
            'Latitude Apartments',
            'Octave',
            'Seven07',
            'The Tower at Third',
            'Yugo Urbana Illinois',
            '75 Armory',
            'Illini Manor Apartments',
          ].map((name, i) => (
            <div
              key={name}
              className="px-5 py-5"
              style={{
                borderRight: '1px solid var(--ink)',
                borderBottom: '1px solid var(--ink)',
                background: i % 2 === 0 ? 'var(--cream)' : 'var(--paper)',
                minHeight: '88px',
              }}
            >
              <div
                style={{
                  fontFamily: 'var(--mono)',
                  fontSize: '10.5px',
                  letterSpacing: '0.08em',
                  textTransform: 'uppercase',
                  color: 'var(--ink-3)',
                  marginBottom: '10px',
                }}
              >
                {String(i + 1).padStart(2, '0')}
              </div>
              <div
                style={{
                  fontFamily: 'var(--serif)',
                  fontSize: '22px',
                  fontWeight: 500,
                  lineHeight: 1.15,
                  letterSpacing: '-0.01em',
                }}
              >
                {name}
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* How We Score */}
      <section className="pt-18 pb-0" style={{ paddingTop: '72px' }}>
        <div className="grid gap-10 items-end mb-8" style={{ gridTemplateColumns: '1fr 2fr' }}>
          <div>
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
              § 02 — Trust scoring
            </div>
            <h2
              className="m-0"
              style={{
                fontFamily: 'var(--serif)',
                fontWeight: 500,
                fontSize: '44px',
                lineHeight: 1.05,
                letterSpacing: '-0.02em',
              }}
            >
              How we score trust
            </h2>
          </div>
          <div
            className="ml-auto"
            style={{
              fontFamily: 'var(--serif)',
              fontSize: '18px',
              lineHeight: 1.55,
              color: 'var(--ink-2)',
              maxWidth: '560px',
            }}
          >
            The current demo score is intentionally simple and explainable. We combine review
            sentiment, Reddit evidence, rent value, amenities, utilities, and commute data, then
            keep the source counts visible so students can judge how much evidence supports each
            result.
          </div>
        </div>

        <div
          className="grid"
          style={{
            gridTemplateColumns: 'repeat(5, 1fr)',
            gap: 0,
            borderTop: '1px solid var(--ink)',
            borderBottom: '1px solid var(--ink)',
          }}
        >
          {[
            {
              n: '01',
              title: 'Google sentiment',
              desc: 'Star ratings, review text, owner responses, and VADER sentiment.',
              chip: '35% weight',
              highlight: true,
            },
            {
              n: '02',
              title: 'Reddit signal',
              desc: 'Resolved housing mentions and VADER-scored student comments.',
              chip: '20% weight',
            },
            {
              n: '03',
              title: 'Rent value',
              desc: 'Pricing rows, per-person rent, unit mix, and value score.',
              chip: '20% weight',
            },
            {
              n: '04',
              title: 'Amenities',
              desc: 'Gym, pool, study space, parking, laundry, bike storage, and pets.',
              chip: '15% weight',
            },
            {
              n: '05',
              title: 'Commute',
              desc: 'Walking, biking, and transit time to common campus destinations.',
              chip: '10% weight',
            },
          ].map((card, i, arr) => (
            <div
              key={i}
              className="px-5.5 py-7 relative"
              style={{
                borderRight: i < arr.length - 1 ? '1px solid var(--rule)' : '0',
                background: card.highlight ? 'var(--cream-2)' : 'transparent',
              }}
            >
              <div
                style={{
                  fontFamily: 'var(--mono)',
                  fontSize: '11px',
                  color: 'var(--ink-3)',
                  letterSpacing: '0.08em',
                }}
              >
                {card.n}
              </div>
              <h3
                className="my-3"
                style={{
                  fontFamily: 'var(--serif)',
                  fontWeight: 500,
                  fontSize: '22px',
                  lineHeight: 1.15,
                  letterSpacing: '-0.01em',
                }}
              >
                {card.title}
              </h3>
              <p
                className="mb-4.5"
                style={{
                  fontSize: '13.5px',
                  lineHeight: 1.5,
                  color: 'var(--ink-2)',
                  margin: '0 0 18px',
                }}
              >
                {card.desc}
              </p>
              <span
                className="inline-block px-1.5 py-0.5"
                style={{
                  fontFamily: 'var(--mono)',
                  fontSize: '10.5px',
                  letterSpacing: '0.06em',
                  border: '1px solid var(--ink)',
                  textTransform: 'uppercase',
                }}
              >
                {card.chip}
              </span>
            </div>
          ))}
        </div>
      </section>

      {/* Browse Modes */}
      <section className="pt-18" style={{ paddingTop: '72px' }}>
        <div className="grid gap-10 items-end mb-8" style={{ gridTemplateColumns: '1fr 2fr' }}>
          <div>
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
              § 03 — Start your search
            </div>
            <h2
              className="m-0"
              style={{
                fontFamily: 'var(--serif)',
                fontWeight: 500,
                fontSize: '44px',
                lineHeight: 1.05,
                letterSpacing: '-0.02em',
              }}
            >
              Three ways in.
            </h2>
          </div>
          <div
            className="ml-auto"
            style={{
              fontFamily: 'var(--serif)',
              fontSize: '18px',
              lineHeight: 1.55,
              color: 'var(--ink-2)',
              maxWidth: '560px',
            }}
          >
            We don't think there's one right apartment for every student. Pick the question that
            matters most to you, and the filters will start there.
          </div>
        </div>

        <div
          className="grid"
          style={{
            gridTemplateColumns: 'repeat(3, 1fr)',
            gap: 0,
            borderTop: '1px solid var(--ink)',
            borderBottom: '1px solid var(--ink)',
          }}
        >
          {[
            {
              mode: 'Mode A',
              title: 'By commute',
              desc: '"I spend most days near Grainger or Siebel. Show me places where the walk will not become a daily tax."',
            },
            {
              mode: 'Mode B',
              title: 'By value',
              desc: '"I care about rent, but I also want to know what that rent actually includes."',
            },
            {
              mode: 'Mode C',
              title: 'By trust',
              desc: '"Do students sound satisfied after move-in, or do the reviews turn negative once management gets involved?"',
            },
          ].map((cell, i, arr) => (
            <button
              key={i}
              onClick={i === 0 ? onNavigateToMap : undefined}
              className="px-7 py-8 cursor-pointer transition-colors text-left"
              style={{
                borderRight: i < arr.length - 1 ? '1px solid var(--rule)' : '0',
                background: 'transparent',
                border: 'none',
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = 'var(--paper)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = 'transparent';
              }}
            >
              <div
                style={{
                  fontFamily: 'var(--mono)',
                  fontSize: '11px',
                  letterSpacing: '0.08em',
                  color: 'var(--orange)',
                  textTransform: 'uppercase',
                }}
              >
                {cell.mode}
              </div>
              <h3
                className="my-2.5"
                style={{
                  fontFamily: 'var(--serif)',
                  fontWeight: 500,
                  fontSize: '30px',
                  lineHeight: 1.1,
                  letterSpacing: '-0.015em',
                  margin: '10px 0 14px',
                }}
              >
                {cell.title}
              </h3>
              <p
                className="mb-4.5"
                style={{
                  fontSize: '13.5px',
                  color: 'var(--ink-2)',
                  margin: '0 0 18px',
                  maxWidth: '320px',
                }}
              >
                {cell.desc}
              </p>
              <span
                className="inline-flex items-center pb-0.5"
                style={{
                  fontFamily: 'var(--mono)',
                  fontSize: '11px',
                  letterSpacing: '0.08em',
                  textTransform: 'uppercase',
                  gap: '8px',
                  borderBottom: '1px solid var(--ink)',
                }}
              >
                Browse by building →
              </span>
            </button>
          ))}
        </div>
      </section>
    </div>
  );
}
