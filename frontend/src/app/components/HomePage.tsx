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
            <div>7 undergrads, 1 grad</div>
            <div style={{ marginTop: '14px' }}>
              <b style={{ color: 'var(--ink)', fontWeight: 500 }}>LAST UPDATE</b>
            </div>
            <div>Apr 27, 2026 · 06:00 CT</div>
            <div style={{ marginTop: '14px' }}>
              <b style={{ color: 'var(--ink)', fontWeight: 500 }}>COVERAGE</b>
            </div>
            <div>412 buildings · 8,940 units</div>
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
            e scraped nine years of r/UIUC, parsed 41,200 Google reviews for synthetic patterns,
            pulled BTAA bus GPS traces, and trained a small regression on four years of Zillow and
            Redfin history. Then we threw out every building where the signal was too weak to say
            anything honest. What's left is the list below — the only one we'd show our younger
            siblings.
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
            { num: '412', unit: 'bldgs', label: 'Properties indexed across Champaign–Urbana' },
            { num: '8,940', unit: 'units', label: 'Individual apartments & bedrooms tracked' },
            { num: '41.2k', unit: 'reviews', label: 'Google reviews parsed for signal' },
            { num: '9', unit: 'years', label: 'Of Reddit data scraped from r/UIUC' },
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
              § 01 — Trust scoring
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
            A building's trust score reflects how likely we think it is to deliver what it promises.
            We weight five signals, tuned over two years of student feedback.
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
              title: 'Review honesty',
              desc: 'Google review velocity, text patterns, verified stays.',
              chip: '35% weight',
              highlight: true,
            },
            {
              n: '02',
              title: 'Lease clarity',
              desc: 'Hidden fees, deposit history, sublease restrictions.',
              chip: '25% weight',
            },
            {
              n: '03',
              title: 'Maintenance',
              desc: 'Response time, Reddit mentions, work order patterns.',
              chip: '20% weight',
            },
            {
              n: '04',
              title: 'Value signal',
              desc: 'Rent vs. comparable units, historic pricing trends.',
              chip: '15% weight',
            },
            {
              n: '05',
              title: 'Management',
              desc: 'Turnover, Better Business Bureau, legal filings.',
              chip: '5% weight',
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
              § 02 — Start your search
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
              desc: '"I\'m Materials Science. I\'m in MSEB every day. Show me places where I\'ll never lose more than 12 minutes to winter."',
            },
            {
              mode: 'Mode B',
              title: 'By price honesty',
              desc: '"I have $1,150/mo. Don\'t show me places asking for that with a straight face. Show me the ones that deserve it."',
            },
            {
              mode: 'Mode C',
              title: 'By trust',
              desc: '"I don\'t care about the pool. I care that the landlord will return my security deposit. Sort by that."',
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

      {/* Methodology */}
      <section className="pt-18 pb-0" style={{ paddingTop: '72px' }}>
        <div className="grid gap-10 items-end mb-5.5" style={{ gridTemplateColumns: '1fr 2fr' }}>
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
              § 03 — A note on methodology
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
              How this was built
            </h2>
          </div>
        </div>

        <div
          className="grid gap-14 pt-5.5"
          style={{
            gridTemplateColumns: '1fr 1fr',
            borderTop: '1px solid var(--ink)',
          }}
        >
          <div>
            <h3
              className="mb-3.5"
              style={{
                fontFamily: 'var(--serif)',
                fontWeight: 500,
                fontSize: '28px',
                letterSpacing: '-0.01em',
                margin: '0 0 14px',
              }}
            >
              Data sources
            </h3>
            <p
              style={{
                fontSize: '14.5px',
                lineHeight: 1.6,
                color: 'var(--ink-2)',
                margin: '0 0 14px',
              }}
            >
              We pull from Google Maps reviews, r/UIUC archives (2016–present), Zillow rental
              history, CUMTD bus GPS, and Better Business Bureau complaints. Everything is
              re-scraped weekly except Reddit, which updates daily.
            </p>
            <ul
              className="list-none p-0 mt-3.5"
              style={{
                fontFamily: 'var(--mono)',
                fontSize: '11.5px',
                color: 'var(--ink-3)',
                letterSpacing: '0.02em',
                textTransform: 'uppercase',
                margin: '14px 0 0',
              }}
            >
              {[
                { label: 'Google reviews', val: '41,200 parsed' },
                { label: 'Reddit posts', val: '2,847 analyzed' },
                { label: 'Zillow listings', val: '4 years history' },
                { label: 'Bus traces', val: '12mo GPS data' },
              ].map((item, i) => (
                <li
                  key={i}
                  className="flex justify-between py-2"
                  style={{
                    padding: '8px 0',
                    borderTop: i === 0 ? '0' : '1px solid var(--rule)',
                  }}
                >
                  <span>{item.label}</span>
                  <b style={{ color: 'var(--ink)', fontWeight: 500 }}>{item.val}</b>
                </li>
              ))}
            </ul>
          </div>

          <div>
            <h3
              className="mb-3.5"
              style={{
                fontFamily: 'var(--serif)',
                fontWeight: 500,
                fontSize: '28px',
                letterSpacing: '-0.01em',
                margin: '0 0 14px',
              }}
            >
              Why we built this
            </h3>
            <p
              style={{
                fontSize: '14.5px',
                lineHeight: 1.6,
                color: 'var(--ink-2)',
                margin: '0 0 14px',
              }}
            >
              Every apartment site we found was pay-to-play. Landlords buy better placement. Reviews
              disappear. Pricing is opaque. We got tired of it, so we built the tool we wish existed
              when we were freshmen.
            </p>
            <p
              style={{
                fontSize: '14.5px',
                lineHeight: 1.6,
                color: 'var(--ink-2)',
                margin: '0 0 14px',
              }}
            >
              This is a research project. We don't take money from landlords, management companies,
              or real estate platforms. Our code is open source. If you find a bug or want to
              contribute data, email us at{' '}
              <span style={{ fontFamily: 'var(--mono)' }}>hello@illininest.com</span>.
            </p>
          </div>
        </div>
      </section>
    </div>
  );
}
