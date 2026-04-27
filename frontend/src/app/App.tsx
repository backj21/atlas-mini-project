import { useState, useEffect } from 'react';
import { Masthead } from './components/Masthead';
import { HomePage } from './components/HomePage';
import { BuildingSelector } from './components/BuildingSelector';
import { ComplexList } from './components/ComplexList';
import { MapView } from './components/MapView';
import { Footer } from './components/Footer';

interface ApartmentComplex {
  id: string;
  name: string;
  lat: number;
  lng: number;
  trustScore: number;
  walkTime: number;
  bikeTime: number;
}

interface Building {
  id: string;
  name: string;
  lat: number;
  lng: number;
}

type Route = 'home' | 'map';

// Mock data for UIUC campus buildings
const campusBuildings: Building[] = [
  { id: 'siebel', name: 'Siebel Center (CS)', lat: 40.1138, lng: -88.2249 },
  { id: 'grainger', name: 'Grainger Library', lat: 40.1125, lng: -88.2267 },
  { id: 'eceb', name: 'ECEB', lat: 40.1149, lng: -88.2284 },
  { id: 'union', name: 'Illini Union', lat: 40.1093, lng: -88.2272 },
  { id: 'krannert', name: 'Krannert Center', lat: 40.1080, lng: -88.2223 },
];

// Mock data for apartment complexes near UIUC
const apartmentComplexes: ApartmentComplex[] = [
  { id: '1', name: 'Hendrick House', lat: 40.1105, lng: -88.2295, trustScore: 8.4, walkTime: 7, bikeTime: 3 },
  { id: '2', name: '309 Green St', lat: 40.1102, lng: -88.2310, trustScore: 5.9, walkTime: 4, bikeTime: 2 },
  { id: '3', name: 'Illini Tower', lat: 40.1055, lng: -88.2285, trustScore: 6.7, walkTime: 12, bikeTime: 5 },
  { id: '4', name: 'Lincoln Square', lat: 40.1165, lng: -88.2305, trustScore: 7.1, walkTime: 9, bikeTime: 4 },
  { id: '5', name: 'Campustown Arms', lat: 40.1115, lng: -88.2330, trustScore: 4.8, walkTime: 5, bikeTime: 2 },
  { id: '6', name: 'Green & Armory', lat: 40.1048, lng: -88.2198, trustScore: 6.2, walkTime: 15, bikeTime: 6 },
  { id: '7', name: 'University Commons', lat: 40.1180, lng: -88.2260, trustScore: 7.8, walkTime: 11, bikeTime: 4 },
  { id: '8', name: 'JSM Apartments', lat: 40.1095, lng: -88.2350, trustScore: 5.3, walkTime: 8, bikeTime: 3 },
  { id: '9', name: 'Here Urbana', lat: 40.1125, lng: -88.2190, trustScore: 8.2, walkTime: 10, bikeTime: 4 },
  { id: '10', name: 'The Retreat', lat: 40.1200, lng: -88.2320, trustScore: 6.9, walkTime: 16, bikeTime: 6 },
];

export default function App() {
  const [currentRoute, setCurrentRoute] = useState<Route>('home');
  const [selectedBuilding, setSelectedBuilding] = useState<Building>(campusBuildings[0]);
  const [transportMode, setTransportMode] = useState<'walk' | 'bike'>('walk');
  const [selectedComplexId, setSelectedComplexId] = useState<string | null>('1');
  const [isDarkMode, setIsDarkMode] = useState(() => {
    const saved = localStorage.getItem('darkMode');
    return saved ? JSON.parse(saved) : false;
  });

  useEffect(() => {
    if (isDarkMode) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
    localStorage.setItem('darkMode', JSON.stringify(isDarkMode));
  }, [isDarkMode]);

  return (
    <div className="w-full min-h-screen" style={{ background: 'var(--background)' }}>
      <Masthead
        isDark={isDarkMode}
        onToggleDark={() => setIsDarkMode(!isDarkMode)}
        currentRoute={currentRoute}
        onNavigate={setCurrentRoute}
      />

      {currentRoute === 'home' ? (
        <>
          <HomePage onNavigateToMap={() => setCurrentRoute('map')} />
          <Footer />
        </>
      ) : (
        <>
          <div className="mx-auto px-8" style={{ maxWidth: '1280px' }}>
            {/* Page Header */}
            <div className="py-6" style={{ borderBottom: `1px solid var(--ink)` }}>
              <div
                className="mb-1"
                style={{
                  fontFamily: 'var(--mono)',
                  fontSize: '11px',
                  letterSpacing: '0.06em',
                  textTransform: 'uppercase',
                  color: 'var(--ink-3)',
                }}
              >
                § Browse — Champaign–Urbana · {apartmentComplexes.length} buildings indexed
              </div>
              <h2
                className="m-0"
                style={{
                  fontFamily: 'var(--serif)',
                  fontSize: '32px',
                  fontWeight: 500,
                  letterSpacing: '-0.02em',
                }}
              >
                Find your place by commute
              </h2>
            </div>

            {/* Search Body - Map view */}
            <div className="grid gap-14 py-10" style={{ gridTemplateColumns: '1fr 420px' }}>
              {/* Left: List */}
              <div>
                <BuildingSelector
                  buildings={campusBuildings}
                  selectedBuilding={selectedBuilding}
                  onBuildingChange={setSelectedBuilding}
                  transportMode={transportMode}
                  onTransportModeChange={setTransportMode}
                />

                <div
                  className="flex items-center justify-between py-3"
                  style={{
                    fontFamily: 'var(--mono)',
                    fontSize: '11px',
                    letterSpacing: '0.04em',
                    textTransform: 'uppercase',
                    color: 'var(--ink-3)',
                    marginTop: '20px',
                  }}
                >
                  <div>
                    <b style={{ color: 'var(--ink)' }}>{apartmentComplexes.length}</b> buildings · sorted by trust
                  </div>
                  <div>Showing walk time from {selectedBuilding.name}</div>
                </div>

                <ComplexList
                  complexes={apartmentComplexes}
                  selectedComplexId={selectedComplexId}
                  onComplexSelect={setSelectedComplexId}
                  transportMode={transportMode}
                />
              </div>

              {/* Right: Map Rail */}
              <aside className="sticky self-start" style={{ top: '130px' }}>
                <MapView
                  complexes={apartmentComplexes}
                  selectedComplexId={selectedComplexId}
                  onComplexSelect={setSelectedComplexId}
                  campusBuilding={selectedBuilding}
                  transportMode={transportMode}
                  isDarkMode={isDarkMode}
                />
              </aside>
            </div>
          </div>

          <Footer />
        </>
      )}
    </div>
  );
}