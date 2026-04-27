import { Moon, Sun } from 'lucide-react';

interface MastheadProps {
  isDark: boolean;
  onToggleDark: () => void;
  currentRoute: 'home' | 'map';
  onNavigate: (route: 'home' | 'map') => void;
}

export function Masthead({ isDark, onToggleDark, currentRoute, onNavigate }: MastheadProps) {
  return (
    <header
      className="sticky top-0 z-40"
      style={{
        borderBottom: `1px solid var(--ink)`,
        background: 'var(--cream)',
      }}
    >
      {/* Top dateline */}
      <div
        className="flex items-center justify-between px-8 py-2.5"
        style={{
          borderBottom: `1px solid var(--rule)`,
          fontFamily: 'var(--mono)',
          fontSize: '11px',
          letterSpacing: '0.05em',
          textTransform: 'uppercase',
          color: 'var(--ink-3)',
        }}
      >
        <div className="flex items-center" style={{ gap: '18px' }}>
          <span className="flex items-center">
            <span
              className="inline-block w-1.5 h-1.5 rounded-full mr-1.5"
              style={{ background: 'var(--orange)', verticalAlign: '1px' }}
            />
            Data refreshed — Mon 27 Apr 2026, 06:00 CT
          </span>
          <span>Vol. II · Issue 14</span>
          <span>Champaign–Urbana, IL</span>
        </div>
        <div className="flex items-center" style={{ gap: '16px' }}>
          <span>Built by students · Not affiliated with UIUC</span>
          <button
            onClick={onToggleDark}
            className="p-1.5 hover:opacity-70 transition-opacity"
            aria-label="Toggle dark mode"
            style={{ color: 'var(--ink-3)', background: 'none', border: 'none', cursor: 'pointer' }}
          >
            {isDark ? <Sun size={14} /> : <Moon size={14} />}
          </button>
        </div>
      </div>

      {/* Main masthead */}
      <div
        className="grid items-center gap-6 px-8"
        style={{
          gridTemplateColumns: '1fr auto 1fr',
          padding: '18px 32px 14px 32px',
        }}
      >
        {/* Left nav */}
        <nav className="flex" style={{ gap: '22px' }}>
          {[
            { label: 'Home', route: 'home' as const },
            { label: 'Browse', route: 'map' as const },
            { label: 'Map', route: 'map' as const },
            { label: 'Methodology', route: 'home' as const },
          ].map((item) => {
            const isActive =
              (item.label === 'Home' && currentRoute === 'home') ||
              ((item.label === 'Browse' || item.label === 'Map') && currentRoute === 'map');

            return (
              <button
                key={item.label}
                onClick={() => onNavigate(item.route)}
                className="no-underline py-1 transition-colors"
                style={{
                  fontFamily: 'var(--mono)',
                  fontSize: '11.5px',
                  letterSpacing: '0.08em',
                  textTransform: 'uppercase',
                  color: 'var(--ink)',
                  background: 'none',
                  border: 'none',
                  borderBottom: isActive ? '1px solid var(--orange)' : '1px solid transparent',
                  cursor: 'pointer',
                  padding: '4px 0',
                }}
                onMouseEnter={(e) => {
                  if (!isActive) {
                    e.currentTarget.style.borderBottomColor = 'var(--ink)';
                  }
                }}
                onMouseLeave={(e) => {
                  if (!isActive) {
                    e.currentTarget.style.borderBottomColor = 'transparent';
                  }
                }}
              >
                {item.label}
              </button>
            );
          })}
        </nav>

        {/* Brand */}
        <button
          onClick={() => onNavigate('home')}
          className="text-center cursor-pointer leading-none"
          style={{
            fontFamily: 'var(--serif)',
            fontWeight: 600,
            fontSize: '40px',
            letterSpacing: '-0.02em',
            background: 'none',
            border: 'none',
            color: 'var(--ink)',
          }}
        >
          Illini<span style={{ color: 'var(--orange)' }}>·</span>Nest
        </button>

        {/* Right */}
        <div className="flex justify-end">
          <button
            onClick={() => onNavigate('map')}
            className="px-2.5 py-1.5 cursor-pointer transition-colors"
            style={{
              fontFamily: 'var(--mono)',
              fontSize: '11px',
              letterSpacing: '0.06em',
              textTransform: 'uppercase',
              border: '1px solid var(--ink)',
              background: 'transparent',
              color: 'var(--ink)',
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = 'var(--ink)';
              e.currentTarget.style.color = 'var(--cream)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = 'transparent';
              e.currentTarget.style.color = 'var(--ink)';
            }}
          >
            Find a place →
          </button>
        </div>
      </div>
    </header>
  );
}
