export function Footer() {
  return (
    <footer
      className="mt-20 pt-10 pb-15"
      style={{
        borderTop: '1px solid var(--ink)',
        fontFamily: 'var(--mono)',
        fontSize: '11px',
        letterSpacing: '0.04em',
        color: 'var(--ink-3)',
      }}
    >
      <div className="mx-auto px-8" style={{ maxWidth: '1280px' }}>
        <div className="grid gap-10 mb-10" style={{ gridTemplateColumns: '2fr 1fr 1fr 1fr' }}>
          {/* About */}
          <div>
            <div
              className="mb-3"
              style={{
                fontFamily: 'var(--serif)',
                fontSize: '22px',
                color: 'var(--ink)',
                lineHeight: '1.3',
                letterSpacing: '-0.01em',
                fontWeight: 500,
                textTransform: 'none',
                maxWidth: '360px',
              }}
            >
              Student housing research for UIUC, built by students who've been burned before.
            </div>
          </div>

          {/* Browse */}
          <div>
            <h4
              className="mb-3"
              style={{
                fontFamily: 'var(--mono)',
                fontSize: '11px',
                textTransform: 'uppercase',
                letterSpacing: '0.1em',
                margin: '0 0 12px',
                color: 'var(--ink)',
              }}
            >
              Browse
            </h4>
            <a
              href="#"
              className="block no-underline py-0.5"
              style={{
                color: 'var(--ink-3)',
                textTransform: 'none',
                letterSpacing: 0,
                fontSize: '12px',
              }}
            >
              By trust score
            </a>
            <a
              href="#"
              className="block no-underline py-0.5"
              style={{
                color: 'var(--ink-3)',
                textTransform: 'none',
                letterSpacing: 0,
                fontSize: '12px',
              }}
            >
              By commute
            </a>
            <a
              href="#"
              className="block no-underline py-0.5"
              style={{
                color: 'var(--ink-3)',
                textTransform: 'none',
                letterSpacing: 0,
                fontSize: '12px',
              }}
            >
              By neighborhood
            </a>
            <a
              href="#"
              className="block no-underline py-0.5"
              style={{
                color: 'var(--ink-3)',
                textTransform: 'none',
                letterSpacing: 0,
                fontSize: '12px',
              }}
            >
              Map view
            </a>
          </div>

          {/* About */}
          <div>
            <h4
              className="mb-3"
              style={{
                fontFamily: 'var(--mono)',
                fontSize: '11px',
                textTransform: 'uppercase',
                letterSpacing: '0.1em',
                margin: '0 0 12px',
                color: 'var(--ink)',
              }}
            >
              About
            </h4>
            <a
              href="#"
              className="block no-underline py-0.5"
              style={{
                color: 'var(--ink-3)',
                textTransform: 'none',
                letterSpacing: 0,
                fontSize: '12px',
              }}
            >
              How we score
            </a>
            <a
              href="#"
              className="block no-underline py-0.5"
              style={{
                color: 'var(--ink-3)',
                textTransform: 'none',
                letterSpacing: 0,
                fontSize: '12px',
              }}
            >
              Methodology
            </a>
            <a
              href="#"
              className="block no-underline py-0.5"
              style={{
                color: 'var(--ink-3)',
                textTransform: 'none',
                letterSpacing: 0,
                fontSize: '12px',
              }}
            >
              The team
            </a>
            <a
              href="#"
              className="block no-underline py-0.5"
              style={{
                color: 'var(--ink-3)',
                textTransform: 'none',
                letterSpacing: 0,
                fontSize: '12px',
              }}
            >
              Data sources
            </a>
          </div>

          {/* Contact */}
          <div>
            <h4
              className="mb-3"
              style={{
                fontFamily: 'var(--mono)',
                fontSize: '11px',
                textTransform: 'uppercase',
                letterSpacing: '0.1em',
                margin: '0 0 12px',
                color: 'var(--ink)',
              }}
            >
              Contact
            </h4>
            <a
              href="#"
              className="block no-underline py-0.5"
              style={{
                color: 'var(--ink-3)',
                textTransform: 'none',
                letterSpacing: 0,
                fontSize: '12px',
              }}
            >
              Suggest a correction
            </a>
            <a
              href="#"
              className="block no-underline py-0.5"
              style={{
                color: 'var(--ink-3)',
                textTransform: 'none',
                letterSpacing: 0,
                fontSize: '12px',
              }}
            >
              Report a building
            </a>
            <a
              href="#"
              className="block no-underline py-0.5"
              style={{
                color: 'var(--ink-3)',
                textTransform: 'none',
                letterSpacing: 0,
                fontSize: '12px',
              }}
            >
              GitHub
            </a>
          </div>
        </div>

        {/* Bottom */}
        <div
          className="flex justify-between pt-5"
          style={{
            borderTop: '1px solid var(--rule)',
          }}
        >
          <div>© 2026 IlliniNest · Student research project</div>
          <div>Not affiliated with UIUC or any property management company</div>
        </div>
      </div>
    </footer>
  );
}
