interface ApartmentComplex {
  id: string;
  name: string;
  address: string;
  city: string;
  lat: number;
  lng: number;
  trustScore: number;
  walkTime: number;
  bikeTime: number;
}

interface ComplexListProps {
  complexes: ApartmentComplex[];
  selectedComplexId: string | null;
  onComplexSelect: (id: string) => void;
  transportMode: 'walk' | 'bike';
}

export function ComplexList({
  complexes,
  selectedComplexId,
  onComplexSelect,
  transportMode,
}: ComplexListProps) {
  const getTrustColor = (score: number) => {
    if (score >= 7.5) return 'var(--green)';
    if (score >= 5) return 'var(--amber)';
    return 'var(--red)';
  };

  const getTrustLabel = (score: number) => {
    if (score >= 8) return 'High trust';
    if (score >= 6.5) return 'Good';
    if (score >= 5) return 'Fair';
    return 'Caution';
  };

  return (
    <div className="flex flex-col" style={{ marginTop: '16px', gap: '12px' }}>
      {complexes.map((complex) => {
        const isSelected = complex.id === selectedComplexId;
        const time = transportMode === 'walk' ? complex.walkTime : complex.bikeTime;
        const trustColor = getTrustColor(complex.trustScore);
        const trustLabel = getTrustLabel(complex.trustScore);

        return (
          <button
            key={complex.id}
            onClick={() => onComplexSelect(complex.id)}
            className="w-full text-left transition-all cursor-pointer"
            style={{
              border: isSelected ? '1px solid var(--ink)' : '1px solid var(--rule)',
              background: isSelected ? 'var(--paper)' : 'var(--cream)',
              padding: '16px 18px',
            }}
            onMouseEnter={(e) => {
              if (!isSelected) {
                e.currentTarget.style.background = 'var(--paper)';
              }
            }}
            onMouseLeave={(e) => {
              if (!isSelected) {
                e.currentTarget.style.background = 'var(--cream)';
              }
            }}
          >
            {/* Header row - name and trust score */}
            <div className="flex items-start justify-between mb-2">
              <h3
                className="m-0"
                style={{
                  fontFamily: 'var(--serif)',
                  fontSize: '20px',
                  fontWeight: 500,
                  letterSpacing: '-0.01em',
                  lineHeight: '1.2',
                  color: 'var(--ink)',
                }}
              >
                {complex.name}
              </h3>
              <div className="text-right">
                <div
                  style={{
                    fontFamily: 'var(--serif)',
                    fontSize: '28px',
                    fontWeight: 500,
                    letterSpacing: '-0.015em',
                    lineHeight: '1',
                    color: trustColor,
                  }}
                >
                  {complex.trustScore.toFixed(1)}
                  <span
                    style={{
                      fontFamily: 'var(--mono)',
                      fontSize: '11px',
                      color: 'var(--ink-3)',
                      marginLeft: '3px',
                      verticalAlign: '3px',
                    }}
                  >
                    /10
                  </span>
                </div>
                <div
                  style={{
                    fontFamily: 'var(--mono)',
                    fontSize: '10.5px',
                    letterSpacing: '0.08em',
                    textTransform: 'uppercase',
                    color: 'var(--ink-3)',
                    marginTop: '4px',
                  }}
                >
                  {trustLabel}
                </div>
              </div>
            </div>

            {/* Details row */}
            <div
              className="flex items-center"
              style={{
                fontFamily: 'var(--mono)',
                fontSize: '11.5px',
                color: 'var(--ink-2)',
                paddingTop: '10px',
                borderTop: '1px solid var(--rule)',
                gap: '12px',
              }}
            >
              <span>
                <b style={{ color: 'var(--ink)', fontWeight: 500 }}>{time} min</b> {transportMode}
              </span>
              <span style={{ color: 'var(--ink-4)' }}>·</span>
              <span>
                {complex.address} · {complex.city}
              </span>
            </div>
          </button>
        );
      })}
    </div>
  );
}
