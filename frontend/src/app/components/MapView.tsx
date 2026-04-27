import { useEffect, useState } from 'react';

interface ApartmentComplex {
  id: string;
  name: string;
  lat: number;
  lng: number;
  trustScore: number;
  walkTime: number;
  bikeTime: number;
}

interface MapViewProps {
  complexes: ApartmentComplex[];
  selectedComplexId: string | null;
  onComplexSelect: (id: string) => void;
  campusBuilding: { lat: number; lng: number; name: string };
  transportMode: 'walk' | 'bike';
  isDarkMode: boolean;
}

export function MapView({
  complexes,
  selectedComplexId,
  onComplexSelect,
  transportMode,
  isDarkMode,
}: MapViewProps) {
  // Convert real coordinates to abstract map positions (0-100 scale)
  const normalizePosition = (lat: number, lng: number) => {
    // UIUC approximate bounds
    const latMin = 40.104;
    const latMax = 40.121;
    const lngMin = -88.235;
    const lngMax = -88.218;

    const x = ((lng - lngMin) / (lngMax - lngMin)) * 100;
    const y = ((latMax - lat) / (latMax - latMin)) * 100;

    return { x: Math.max(0, Math.min(100, x)), y: Math.max(0, Math.min(100, y)) };
  };

  const getTrustColorClass = (score: number) => {
    if (score >= 8) return 'hi';
    if (score >= 6) return 'mid';
    return 'lo';
  };

  return (
    <div>
      {/* Map Container */}
      <div
        className="relative overflow-hidden"
        style={{
          background: 'var(--paper)',
          border: '1px solid var(--ink)',
          aspectRatio: '1 / 1.05',
        }}
      >
        {/* Grid background */}
        <div
          className="absolute inset-0"
          style={{
            backgroundImage:
              'linear-gradient(var(--rule-2) 1px, transparent 1px), linear-gradient(90deg, var(--rule-2) 1px, transparent 1px)',
            backgroundSize: '40px 40px',
            opacity: 0.7,
          }}
        />

        {/* Campus area (Main Quad) */}
        <div
          className="absolute"
          style={{
            left: '30%',
            top: '40%',
            width: '34%',
            height: '28%',
            background: isDarkMode ? 'rgba(90, 127, 168, 0.12)' : 'rgba(19, 41, 75, 0.06)',
            border: '1px dashed var(--blue)',
          }}
        >
          <div
            className="absolute"
            style={{
              top: '50%',
              left: '50%',
              transform: 'translate(-50%, -50%)',
              fontFamily: 'var(--mono)',
              fontSize: '10px',
              color: 'var(--blue)',
              letterSpacing: '0.12em',
              textAlign: 'center',
            }}
          >
            MAIN QUAD
          </div>
        </div>

        {/* Street lines */}
        <div
          className="absolute left-0 right-0"
          style={{ top: '22%', height: '1.5px', background: 'var(--rule)' }}
        />
        <div
          className="absolute left-0 right-0"
          style={{ top: '54%', height: '1.5px', background: 'var(--rule)' }}
        />
        <div
          className="absolute left-0 right-0"
          style={{ top: '78%', height: '1.5px', background: 'var(--rule)' }}
        />
        <div
          className="absolute top-0 bottom-0"
          style={{ left: '24%', width: '1.5px', background: 'var(--rule)' }}
        />
        <div
          className="absolute top-0 bottom-0"
          style={{ left: '52%', width: '1.5px', background: 'var(--rule)' }}
        />
        <div
          className="absolute top-0 bottom-0"
          style={{ left: '74%', width: '1.5px', background: 'var(--rule)' }}
        />

        {/* Apartment pins */}
        {complexes.map((complex) => {
          const pos = normalizePosition(complex.lat, complex.lng);
          const trustClass = getTrustColorClass(complex.trustScore);
          const isSelected = complex.id === selectedComplexId;

          return (
            <button
              key={complex.id}
              onClick={() => onComplexSelect(complex.id)}
              className="absolute transition-transform cursor-pointer"
              style={{
                left: `${pos.x}%`,
                top: `${pos.y}%`,
                transform: 'translate(-50%, -50%)',
                width: '28px',
                height: '28px',
                borderRadius: '50%',
                border: isSelected ? '1.5px solid var(--ink)' : '1.5px solid var(--ink)',
                background:
                  trustClass === 'hi'
                    ? 'var(--orange)'
                    : trustClass === 'mid'
                      ? 'var(--cream)'
                      : 'var(--ink-4)',
                color: trustClass === 'hi' ? '#fff' : trustClass === 'mid' ? 'var(--ink)' : '#fff',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                fontFamily: 'var(--mono)',
                fontSize: '11px',
                fontWeight: 600,
                boxShadow: isSelected ? '0 0 0 3px var(--orange)' : 'none',
                zIndex: isSelected ? 10 : 1,
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = 'translate(-50%, -50%) scale(1.15)';
                e.currentTarget.style.zIndex = '5';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.transform = 'translate(-50%, -50%)';
                e.currentTarget.style.zIndex = isSelected ? '10' : '1';
              }}
            >
              {Math.round(complex.trustScore * 10)}
            </button>
          );
        })}
      </div>

      {/* Map legend */}
      <div
        className="flex items-center justify-between pt-2.5"
        style={{
          fontFamily: 'var(--mono)',
          fontSize: '10.5px',
          textTransform: 'uppercase',
          letterSpacing: '0.06em',
          color: 'var(--ink-3)',
        }}
      >
        <span>Fig 2.1 — Trust-weighted map</span>
        <div className="flex items-center" style={{ gap: '14px' }}>
          <span className="flex items-center" style={{ gap: '5px' }}>
            <i
              className="inline-block w-2.5 h-2.5 rounded-full"
              style={{ background: 'var(--orange)', border: '1px solid var(--ink)' }}
            />
            80+
          </span>
          <span className="flex items-center" style={{ gap: '5px' }}>
            <i
              className="inline-block w-2.5 h-2.5 rounded-full"
              style={{ background: 'var(--cream)', border: '1px solid var(--ink)' }}
            />
            60–79
          </span>
          <span className="flex items-center" style={{ gap: '5px' }}>
            <i
              className="inline-block w-2.5 h-2.5 rounded-full"
              style={{ background: 'var(--ink-4)', border: '1px solid var(--ink)' }}
            />
            &lt; 60
          </span>
        </div>
      </div>
    </div>
  );
}
