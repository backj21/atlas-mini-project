import { useState, useRef, useEffect } from 'react';
import { ChevronDown } from 'lucide-react';

interface Building {
  id: string;
  name: string;
  lat: number;
  lng: number;
}

interface BuildingSelectorProps {
  buildings: Building[];
  selectedBuilding: Building;
  onBuildingChange: (building: Building) => void;
  transportMode: 'walk' | 'bike';
  onTransportModeChange: (mode: 'walk' | 'bike') => void;
}

export function BuildingSelector({
  buildings,
  selectedBuilding,
  onBuildingChange,
  transportMode,
  onTransportModeChange,
}: BuildingSelectorProps) {
  const [isOpen, setIsOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  return (
    <div
      className="py-4 px-0"
      style={{
        borderBottom: `1px solid var(--rule)`,
      }}
    >
      <div className="mb-3">
        <div
          className="mb-2"
          style={{
            fontFamily: 'var(--mono)',
            fontSize: '10.5px',
            letterSpacing: '0.08em',
            textTransform: 'uppercase',
            color: 'var(--ink-3)',
          }}
        >
          Showing {transportMode} time from:
        </div>
        <div className="relative" ref={dropdownRef}>
          <button
            onClick={() => setIsOpen(!isOpen)}
            className="flex items-center gap-1"
            style={{
              fontFamily: 'var(--serif)',
              fontSize: '18px',
              fontWeight: 500,
              color: 'var(--ink)',
              letterSpacing: '-0.01em',
              background: 'none',
              border: 'none',
              padding: 0,
              cursor: 'pointer',
            }}
          >
            {selectedBuilding.name}
            <ChevronDown size={16} style={{ color: 'var(--orange)' }} />
          </button>

          {isOpen && (
            <div
              className="absolute top-full left-0 mt-2 min-w-[240px] py-1 z-50"
              style={{
                background: 'var(--paper)',
                border: `1px solid var(--ink)`,
                boxShadow: '0 2px 8px rgba(0, 0, 0, 0.15)',
              }}
            >
              {buildings.map((building) => {
                const isSelected = building.id === selectedBuilding.id;
                return (
                  <button
                    key={building.id}
                    onClick={() => {
                      onBuildingChange(building);
                      setIsOpen(false);
                    }}
                    className="w-full px-3 py-2 text-left transition-colors"
                    style={{
                      fontFamily: 'var(--serif)',
                      fontSize: '15px',
                      color: 'var(--ink)',
                      background: isSelected ? 'var(--cream-2)' : 'transparent',
                      border: 'none',
                      cursor: 'pointer',
                    }}
                    onMouseEnter={(e) => {
                      if (!isSelected) {
                        e.currentTarget.style.background = 'var(--cream-2)';
                      }
                    }}
                    onMouseLeave={(e) => {
                      if (!isSelected) {
                        e.currentTarget.style.background = 'transparent';
                      }
                    }}
                  >
                    {building.name}
                  </button>
                );
              })}
            </div>
          )}
        </div>
      </div>

      <div className="flex items-center gap-2">
        <button
          onClick={() => onTransportModeChange('walk')}
          className="transition-all"
          style={{
            fontFamily: 'var(--mono)',
            fontSize: '11px',
            letterSpacing: '0.06em',
            textTransform: 'uppercase',
            color: transportMode === 'walk' ? 'var(--ink)' : 'var(--ink-3)',
            background: 'none',
            border: 'none',
            padding: 0,
            cursor: 'pointer',
            textDecoration: transportMode === 'walk' ? 'underline' : 'none',
            textUnderlineOffset: '3px',
          }}
        >
          Walk
        </button>
        <span style={{ color: 'var(--ink-4)', fontSize: '11px' }}>/</span>
        <button
          onClick={() => onTransportModeChange('bike')}
          className="transition-all"
          style={{
            fontFamily: 'var(--mono)',
            fontSize: '11px',
            letterSpacing: '0.06em',
            textTransform: 'uppercase',
            color: transportMode === 'bike' ? 'var(--ink)' : 'var(--ink-3)',
            background: 'none',
            border: 'none',
            padding: 0,
            cursor: 'pointer',
            textDecoration: transportMode === 'bike' ? 'underline' : 'none',
            textUnderlineOffset: '3px',
          }}
        >
          Bike
        </button>
      </div>
    </div>
  );
}
