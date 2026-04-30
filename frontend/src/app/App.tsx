import { useMemo, useState, useEffect } from 'react';
import { Masthead } from './components/Masthead';
import { HomePage } from './components/HomePage';
import { MethodologyPage } from './components/MethodologyPage';
import { BuildingSelector } from './components/BuildingSelector';
import { ComplexList } from './components/ComplexList';
import { ApartmentDetailPanel } from './components/ApartmentDetailPanel';
import { MapView } from './components/MapView';
import { Footer } from './components/Footer';
import apartmentDetailsData from './data/apartmentDetails.json';

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

type ApartmentBase = Omit<ApartmentComplex, 'walkTime' | 'bikeTime'>;

interface Building {
  id: string;
  name: string;
  lat: number;
  lng: number;
}

interface CommuteRow {
  building: string;
  walkingMin: number | null;
  bicyclingMin: number | null;
  transitMin: number | null;
}

interface NearbySchoolBuilding {
  building: string;
  lat: number;
  lng: number;
  walkingMin: number | null;
  bicyclingMin: number | null;
  isWalkable: boolean;
  isBikeable: boolean;
}

type Route = 'home' | 'browse' | 'map' | 'methodology';

const WALKING_DISTANCE_MINUTES = 20;
const BIKING_DISTANCE_MINUTES = 8;

const apartmentDetails = apartmentDetailsData as Record<string, { commute: CommuteRow[] }>;

const campusBuildingLocations: Record<string, { lat: number; lng: number }> = {
  'Grainger Engineering Library': { lat: 40.1125, lng: -88.2267 },
  'Siebel Center for CS': { lat: 40.1138, lng: -88.2249 },
  'Illini Union': { lat: 40.1093, lng: -88.2272 },
  CRCE: { lat: 40.1039, lng: -88.2217 },
  ARC: { lat: 40.1014, lng: -88.2356 },
  'Main Library': { lat: 40.1048, lng: -88.229 },
  'Wohlers Hall': { lat: 40.1048, lng: -88.23 },
  'Everitt Lab': { lat: 40.1106, lng: -88.2276 },
  'Loomis Lab': { lat: 40.1107, lng: -88.2237 },
  'Lincoln Hall': { lat: 40.1066, lng: -88.2284 },
  'Krannert Art Center': { lat: 40.108, lng: -88.2223 },
};

function getTravelTime(value: number | null) {
  return value ?? Number.POSITIVE_INFINITY;
}

function getNearbySchoolBuildings(apartmentId: string, transportMode: 'walk' | 'bike'): NearbySchoolBuilding[] {
  const commuteRows = apartmentDetails[apartmentId]?.commute ?? [];

  return commuteRows
    .flatMap((commute) => {
      const location = campusBuildingLocations[commute.building];

      if (!location) {
        return [];
      }

      const isWalkable = getTravelTime(commute.walkingMin) <= WALKING_DISTANCE_MINUTES;
      const isBikeable = getTravelTime(commute.bicyclingMin) <= BIKING_DISTANCE_MINUTES;

      return [{
        building: commute.building,
        lat: location.lat,
        lng: location.lng,
        walkingMin: commute.walkingMin,
        bicyclingMin: commute.bicyclingMin,
        isWalkable,
        isBikeable,
      }];
    })
    .filter((building) => building.isWalkable || building.isBikeable)
    .sort((a, b) => {
      const preferredTimeA = transportMode === 'walk' ? getTravelTime(a.walkingMin) : getTravelTime(a.bicyclingMin);
      const preferredTimeB = transportMode === 'walk' ? getTravelTime(b.walkingMin) : getTravelTime(b.bicyclingMin);
      const fallbackTimeA = Math.min(getTravelTime(a.walkingMin), getTravelTime(a.bicyclingMin));
      const fallbackTimeB = Math.min(getTravelTime(b.walkingMin), getTravelTime(b.bicyclingMin));

      return preferredTimeA - preferredTimeB || fallbackTimeA - fallbackTimeB || a.building.localeCompare(b.building);
    });
}

// Mock data for UIUC campus buildings
const campusBuildings: Building[] = [
  { id: 'siebel', name: 'Siebel Center for CS', lat: 40.1138, lng: -88.2249 },
  { id: 'grainger', name: 'Grainger Engineering Library', lat: 40.1125, lng: -88.2267 },
  { id: 'union', name: 'Illini Union', lat: 40.1093, lng: -88.2272 },
  { id: 'krannert', name: 'Krannert Art Center', lat: 40.1080, lng: -88.2223 },
];

const supportedApartments: ApartmentBase[] = [
  {
    id: 'icon',
    name: 'ICON Apartments',
    address: '309 E Springfield Ave',
    city: 'Champaign',
    lat: 40.1125764,
    lng: -88.2351166,
    trustScore: 7.4,
  },
  {
    id: 'latitude',
    name: 'Latitude Apartments',
    address: '608 E University Ave',
    city: 'Champaign',
    lat: 40.1166547,
    lng: -88.2301839,
    trustScore: 6.7,
  },
  {
    id: 'yugo',
    name: 'Yugo Urbana Illinois',
    address: '410 N Lincoln Ave',
    city: 'Urbana',
    lat: 40.1158408,
    lng: -88.2182043,
    trustScore: 6.6,
  },
  {
    id: 'octave',
    name: 'Octave',
    address: '210 S 4th St',
    city: 'Champaign',
    lat: 40.1149027,
    lng: -88.2328041,
    trustScore: 6.2,
  },
  {
    id: 'armory_75',
    name: '75 Armory',
    address: '75 E Armory Ave',
    city: 'Champaign',
    lat: 40.1049958,
    lng: -88.2389264,
    trustScore: 5.8,
  },
  {
    id: 'illini_manor',
    name: 'Illini Manor Apartments',
    address: '401 E Chalmers St',
    city: 'Champaign',
    lat: 40.1066306,
    lng: -88.2330449,
    trustScore: 5.3,
  },
  {
    id: 'seven07',
    name: 'Seven07',
    address: '707 S 4th St',
    city: 'Champaign',
    lat: 40.1096406,
    lng: -88.2341116,
    trustScore: 5.0,
  },
  {
    id: 'tower_at_third',
    name: 'The Tower at Third',
    address: '302 E John St',
    city: 'Champaign',
    lat: 40.109314,
    lng: -88.2348638,
    trustScore: 4.7,
  },
  {
    id: 'here',
    name: 'HERE Champaign',
    address: '308 E Green St',
    city: 'Champaign',
    lat: 40.1105873,
    lng: -88.2340313,
    trustScore: 4.7,
  },
  {
    id: 'hub',
    name: 'Hub on Campus Champaign',
    address: '812 S 6th St',
    city: 'Champaign',
    lat: 40.108212,
    lng: -88.2297516,
    trustScore: 4.3,
  },
  {
    id: 'dean',
    name: 'The Dean Campustown',
    address: '708 S 6th St',
    city: 'Champaign',
    lat: 40.1098868,
    lng: -88.2300959,
    trustScore: 4.3,
  },
];

const commuteTimes: Record<string, Record<string, { walkTime: number; bikeTime: number }>> = {
  here: {
    grainger: { walkTime: 11, bikeTime: 3 },
    siebel: { walkTime: 16, bikeTime: 5 },
    union: { walkTime: 10, bikeTime: 3 },
    krannert: { walkTime: 17, bikeTime: 4 },
  },
  hub: {
    grainger: { walkTime: 10, bikeTime: 3 },
    siebel: { walkTime: 15, bikeTime: 4 },
    union: { walkTime: 5, bikeTime: 1 },
    krannert: { walkTime: 12, bikeTime: 3 },
  },
  dean: {
    grainger: { walkTime: 8, bikeTime: 4 },
    siebel: { walkTime: 13, bikeTime: 5 },
    union: { walkTime: 6, bikeTime: 2 },
    krannert: { walkTime: 14, bikeTime: 3 },
  },
  icon: {
    grainger: { walkTime: 10, bikeTime: 3 },
    siebel: { walkTime: 15, bikeTime: 6 },
    union: { walkTime: 13, bikeTime: 4 },
    krannert: { walkTime: 20, bikeTime: 5 },
  },
  yugo: {
    grainger: { walkTime: 17, bikeTime: 6 },
    siebel: { walkTime: 10, bikeTime: 2 },
    union: { walkTime: 20, bikeTime: 6 },
    krannert: { walkTime: 37, bikeTime: 9 },
  },
  latitude: {
    grainger: { walkTime: 9, bikeTime: 3 },
    siebel: { walkTime: 10, bikeTime: 3 },
    union: { walkTime: 14, bikeTime: 4 },
    krannert: { walkTime: 26, bikeTime: 7 },
  },
  tower_at_third: {
    grainger: { walkTime: 14, bikeTime: 4 },
    siebel: { walkTime: 20, bikeTime: 6 },
    union: { walkTime: 12, bikeTime: 3 },
    krannert: { walkTime: 16, bikeTime: 4 },
  },
  seven07: {
    grainger: { walkTime: 11, bikeTime: 3 },
    siebel: { walkTime: 17, bikeTime: 5 },
    union: { walkTime: 10, bikeTime: 3 },
    krannert: { walkTime: 15, bikeTime: 4 },
  },
  octave: {
    grainger: { walkTime: 11, bikeTime: 4 },
    siebel: { walkTime: 13, bikeTime: 5 },
    union: { walkTime: 16, bikeTime: 5 },
    krannert: { walkTime: 24, bikeTime: 7 },
  },
  armory_75: {
    grainger: { walkTime: 24, bikeTime: 6 },
    siebel: { walkTime: 30, bikeTime: 8 },
    union: { walkTime: 20, bikeTime: 5 },
    krannert: { walkTime: 14, bikeTime: 4 },
  },
  illini_manor: {
    grainger: { walkTime: 15, bikeTime: 4 },
    siebel: { walkTime: 20, bikeTime: 5 },
    union: { walkTime: 10, bikeTime: 2 },
    krannert: { walkTime: 11, bikeTime: 3 },
  },
};

export default function App() {
  const [currentRoute, setCurrentRoute] = useState<Route>('home');
  const [selectedBuilding, setSelectedBuilding] = useState<Building>(campusBuildings[0]);
  const [transportMode, setTransportMode] = useState<'walk' | 'bike'>('walk');
  const [selectedComplexId, setSelectedComplexId] = useState<string | null>('icon');
  const [isDarkMode, setIsDarkMode] = useState(() => {
    const saved = localStorage.getItem('darkMode');
    return saved ? JSON.parse(saved) : false;
  });

  const apartmentComplexes = useMemo<ApartmentComplex[]>(
    () =>
      supportedApartments.map((apartment) => {
        const commute = commuteTimes[apartment.id]?.[selectedBuilding.id] ?? commuteTimes[apartment.id]?.grainger;

        return {
          ...apartment,
          walkTime: commute?.walkTime ?? 0,
          bikeTime: commute?.bikeTime ?? 0,
        };
      }),
    [selectedBuilding.id],
  );
  const selectedComplex = apartmentComplexes.find((complex) => complex.id === selectedComplexId) ?? apartmentComplexes[0];
  const nearbySchoolBuildingsByComplexId = useMemo<Record<string, NearbySchoolBuilding[]>>(
    () =>
      Object.fromEntries(
        apartmentComplexes.map((complex) => [complex.id, getNearbySchoolBuildings(complex.id, transportMode)]),
      ),
    [apartmentComplexes, transportMode],
  );

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
          <HomePage onNavigateToMap={() => setCurrentRoute('browse')} />
          <Footer />
        </>
      ) : currentRoute === 'browse' ? (
        <>
          <div className="mx-auto px-6" style={{ maxWidth: '1440px' }}>
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
                Browse supported apartments
              </h2>
            </div>

            <div className="grid gap-8 py-10 lg:grid-cols-[minmax(0,1fr)_430px]">
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
                  <button
                    onClick={() => setCurrentRoute('map')}
                    style={{
                      background: 'none',
                      border: 'none',
                      color: 'var(--orange)',
                      cursor: 'pointer',
                      fontFamily: 'var(--mono)',
                      fontSize: '11px',
                      letterSpacing: '0.04em',
                      textTransform: 'uppercase',
                    }}
                  >
                    Open map view
                  </button>
                </div>

                <ComplexList
                  complexes={apartmentComplexes}
                  selectedComplexId={selectedComplexId}
                  onComplexSelect={setSelectedComplexId}
                  transportMode={transportMode}
                />
              </div>

              <ApartmentDetailPanel apartment={selectedComplex} selectedBuildingName={selectedBuilding.name} />
            </div>
          </div>

          <Footer />
        </>
      ) : currentRoute === 'methodology' ? (
        <>
          <MethodologyPage />
          <Footer />
        </>
      ) : (
        <>
          <div className="mx-auto px-8" style={{ maxWidth: '1280px' }}>
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
                § Map — Champaign–Urbana · {apartmentComplexes.length} supported apartments
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
                See supported apartments by location
              </h2>
            </div>

            <div className="grid gap-6 py-8" style={{ gridTemplateColumns: 'minmax(0, 1fr) 280px' }}>
              <MapView
                complexes={apartmentComplexes}
                selectedComplexId={selectedComplexId}
                onComplexSelect={setSelectedComplexId}
                campusBuilding={selectedBuilding}
                transportMode={transportMode}
                isDarkMode={isDarkMode}
                nearbySchoolBuildingsByComplexId={nearbySchoolBuildingsByComplexId}
              />

              <aside
                className="self-start"
                style={{
                  borderLeft: '1px solid var(--rule)',
                  paddingLeft: '20px',
                }}
              >
                <BuildingSelector
                  buildings={campusBuildings}
                  selectedBuilding={selectedBuilding}
                  onBuildingChange={setSelectedBuilding}
                  transportMode={transportMode}
                  onTransportModeChange={setTransportMode}
                />

                <div
                  className="py-5"
                  style={{
                    borderBottom: '1px solid var(--rule)',
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
                    Selected
                  </div>
                  <h3
                    style={{
                      color: 'var(--ink)',
                      fontFamily: 'var(--serif)',
                      fontSize: '24px',
                      fontWeight: 500,
                      letterSpacing: '-0.01em',
                      lineHeight: 1.15,
                      margin: 0,
                    }}
                  >
                    {selectedComplex.name}
                  </h3>
                  <div
                    style={{
                      color: 'var(--ink-3)',
                      fontFamily: 'var(--mono)',
                      fontSize: '11px',
                      lineHeight: 1.5,
                      marginTop: '8px',
                    }}
                  >
                    {selectedComplex.address}, {selectedComplex.city}
                  </div>
                  <div
                    className="grid"
                    style={{
                      gridTemplateColumns: '1fr 1fr',
                      gap: '16px',
                      marginTop: '18px',
                    }}
                  >
                    <div>
                      <div
                        style={{
                          color: 'var(--ink)',
                          fontFamily: 'var(--serif)',
                          fontSize: '26px',
                          fontWeight: 500,
                          lineHeight: 1,
                        }}
                      >
                        {selectedComplex.trustScore.toFixed(1)}
                      </div>
                      <div
                        style={{
                          color: 'var(--ink-3)',
                          fontFamily: 'var(--mono)',
                          fontSize: '10px',
                          letterSpacing: '0.06em',
                          marginTop: '6px',
                          textTransform: 'uppercase',
                        }}
                      >
                        Trust
                      </div>
                    </div>
                    <div>
                      <div
                        style={{
                          color: 'var(--ink)',
                          fontFamily: 'var(--serif)',
                          fontSize: '26px',
                          fontWeight: 500,
                          lineHeight: 1,
                        }}
                      >
                        {transportMode === 'walk' ? selectedComplex.walkTime : selectedComplex.bikeTime}
                      </div>
                      <div
                        style={{
                          color: 'var(--ink-3)',
                          fontFamily: 'var(--mono)',
                          fontSize: '10px',
                          letterSpacing: '0.06em',
                          marginTop: '6px',
                          textTransform: 'uppercase',
                        }}
                      >
                        Min {transportMode}
                      </div>
                    </div>
                  </div>
                </div>

                <div
                  className="py-4"
                  style={{
                    fontFamily: 'var(--mono)',
                    fontSize: '10.5px',
                    letterSpacing: '0.08em',
                    textTransform: 'uppercase',
                    color: 'var(--ink-3)',
                  }}
                >
                  Apartments
                </div>

                <div className="grid" style={{ gap: '2px' }}>
                  {apartmentComplexes.map((complex) => {
                    const isSelected = complex.id === selectedComplexId;
                    const commuteTime = transportMode === 'walk' ? complex.walkTime : complex.bikeTime;

                    return (
                      <button
                        key={complex.id}
                        onClick={() => setSelectedComplexId(complex.id)}
                        className="grid"
                        style={{
                          alignItems: 'center',
                          background: isSelected ? 'var(--paper)' : 'transparent',
                          border: 'none',
                          borderLeft: isSelected ? '2px solid var(--orange)' : '2px solid transparent',
                          color: 'var(--ink)',
                          cursor: 'pointer',
                          gap: '3px',
                          padding: '9px 10px',
                          textAlign: 'left',
                        }}
                      >
                        <span
                          style={{
                            fontFamily: 'var(--serif)',
                            fontSize: '16px',
                            fontWeight: 500,
                            lineHeight: 1.1,
                          }}
                        >
                          {complex.name}
                        </span>
                        <span
                          style={{
                            color: 'var(--ink-3)',
                            fontFamily: 'var(--mono)',
                            fontSize: '10.5px',
                          }}
                        >
                          {commuteTime} min {transportMode} · {complex.trustScore.toFixed(1)} trust
                        </span>
                      </button>
                    );
                  })}
                </div>
              </aside>
            </div>
          </div>

          <Footer />
        </>
      )}
    </div>
  );
}
