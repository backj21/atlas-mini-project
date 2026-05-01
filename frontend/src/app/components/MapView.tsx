import { Fragment, useEffect, useMemo, useState } from 'react';
import type { LatLngBoundsExpression, LatLngExpression } from 'leaflet';
import { CircleMarker, MapContainer, Polyline, Popup, TileLayer, Tooltip, useMap } from 'react-leaflet';

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

interface NearbySchoolBuilding {
  building: string;
  lat: number;
  lng: number;
  walkingMin: number | null;
  bicyclingMin: number | null;
  isWalkable: boolean;
  isBikeable: boolean;
}

interface MapViewProps {
  complexes: ApartmentComplex[];
  selectedComplexId: string | null;
  onComplexSelect: (id: string) => void;
  campusBuilding: { lat: number; lng: number; name: string };
  transportMode: 'walk' | 'bike';
  isDarkMode: boolean;
  nearbySchoolBuildingsByComplexId: Record<string, NearbySchoolBuilding[]>;
}

function FitMapToPoints({
  bounds,
  selectedPoint,
}: {
  bounds: LatLngBoundsExpression;
  selectedPoint: LatLngExpression | null;
}) {
  const map = useMap();

  useEffect(() => {
    map.fitBounds(bounds, { padding: [26, 26], maxZoom: 15 });
  }, [bounds, map]);

  useEffect(() => {
    if (selectedPoint) {
      map.panTo(selectedPoint, { animate: true, duration: 0.45 });
    }
  }, [map, selectedPoint]);

  return null;
}

export function MapView({
  complexes,
  selectedComplexId,
  onComplexSelect,
  campusBuilding,
  transportMode,
  isDarkMode,
  nearbySchoolBuildingsByComplexId,
}: MapViewProps) {
  const [focusedComplexId, setFocusedComplexId] = useState<string | null>(null);
  const isFocused = focusedComplexId !== null;
  const selectedComplex = complexes.find((complex) => complex.id === selectedComplexId);
  const focusedComplex = focusedComplexId ? complexes.find((complex) => complex.id === focusedComplexId) : null;
  const activeComplex = focusedComplex ?? selectedComplex;
  const campusPoint: [number, number] = [campusBuilding.lat, campusBuilding.lng];
  const selectedPoint: LatLngExpression | null = activeComplex ? [activeComplex.lat, activeComplex.lng] : null;
  const selectedNearbySchoolBuildings = activeComplex
    ? nearbySchoolBuildingsByComplexId[activeComplex.id] ?? []
    : [];
  const focusedSchoolBuildings = isFocused ? selectedNearbySchoolBuildings : [];
  const visibleComplexes = isFocused && activeComplex ? [activeComplex] : complexes;
  const bounds = useMemo<LatLngBoundsExpression>(
    () => [
      ...visibleComplexes.map((complex) => [complex.lat, complex.lng] as [number, number]),
      ...focusedSchoolBuildings.map((building) => [building.lat, building.lng] as [number, number]),
      campusPoint,
    ],
    [campusBuilding.lat, campusBuilding.lng, focusedSchoolBuildings, visibleComplexes],
  );

  useEffect(() => {
    if (focusedComplexId && selectedComplexId && focusedComplexId !== selectedComplexId) {
      setFocusedComplexId(selectedComplexId);
    }
  }, [focusedComplexId, selectedComplexId]);

  return (
    <div>
      <div
        className="atlas-map-shell"
        style={{
          background: 'var(--paper)',
          border: '1px solid var(--rule)',
          aspectRatio: '16 / 12',
        }}
      >
        {isFocused ? (
          <button className="atlas-map-focus-reset" onClick={() => setFocusedComplexId(null)}>
            Show all apartments
          </button>
        ) : null}

        <MapContainer
          center={[40.1108, -88.2306]}
          zoom={14}
          scrollWheelZoom
          className={isDarkMode ? 'atlas-map atlas-map-dark' : 'atlas-map'}
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>'
            url={
              isDarkMode
                ? 'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png'
                : 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png'
            }
          />
          <FitMapToPoints bounds={bounds} selectedPoint={selectedPoint} />

          {isFocused && activeComplex
            ? focusedSchoolBuildings.map((building) => {
                const routePositions: [number, number][] = [
                  [activeComplex.lat, activeComplex.lng],
                  [building.lat, building.lng],
                ];
                const routeColor = building.isWalkable ? '#15803d' : '#1d4ed8';

                return (
                  <Fragment key={`${focusedComplexId}-${building.building}-route`}>
                    <Polyline
                      positions={routePositions}
                      pathOptions={{
                        color: '#ffffff',
                        dashArray: '10 9',
                        lineCap: 'round',
                        opacity: 0.95,
                        weight: 6,
                      }}
                    />
                    <Polyline
                      positions={routePositions}
                      pathOptions={{
                        color: routeColor,
                        dashArray: '10 9',
                        lineCap: 'round',
                        opacity: 0.92,
                        weight: 3,
                      }}
                    />
                  </Fragment>
                );
              })
            : null}

          {!isFocused ? (
            <CircleMarker
              center={campusPoint}
              radius={6}
              pathOptions={{
                color: 'var(--blue)',
                fillColor: 'var(--blue)',
                fillOpacity: 0.75,
                weight: 2,
              }}
            >
              <Tooltip direction="top" offset={[0, -8]}>
                {campusBuilding.name}
              </Tooltip>
            </CircleMarker>
          ) : null}

          {focusedSchoolBuildings.map((building) => {
            const preferredTime = transportMode === 'walk' ? building.walkingMin : building.bicyclingMin;

            return (
              <CircleMarker
                key={`${focusedComplexId}-nearby-${building.building}`}
                center={[building.lat, building.lng]}
                radius={7}
                pathOptions={{
                  color: 'var(--paper)',
                  fillColor: building.isWalkable ? 'var(--green)' : 'var(--blue)',
                  fillOpacity: 0.9,
                  opacity: 1,
                  weight: 2,
                }}
              >
                <Tooltip direction="top" offset={[0, -9]}>
                  {building.building}
                </Tooltip>
                <Popup>
                  <div className="atlas-map-popup atlas-map-popup-school">
                    <strong>{building.building}</strong>
                    <span>
                      {building.walkingMin ?? 'N/A'} min walk · {building.bicyclingMin ?? 'N/A'} min bike
                    </span>
                    <span>
                      {preferredTime ?? 'N/A'} min by selected mode from {activeComplex?.name}
                    </span>
                  </div>
                </Popup>
              </CircleMarker>
            );
          })}

          {visibleComplexes.map((complex) => {
            const isSelected = complex.id === selectedComplexId;
            const commuteTime = transportMode === 'walk' ? complex.walkTime : complex.bikeTime;

            return (
              <CircleMarker
                key={complex.id}
                center={[complex.lat, complex.lng]}
                radius={isSelected ? 10 : 6}
                pathOptions={{
                  color: isSelected ? 'var(--orange)' : 'var(--paper)',
                  fillColor: isSelected ? 'var(--orange)' : 'var(--ink)',
                  fillOpacity: isSelected ? 0.95 : 0.72,
                  opacity: 1,
                  weight: isSelected ? 4 : 1.5,
                }}
                eventHandlers={{
                  click: () => {
                    setFocusedComplexId(complex.id);
                    onComplexSelect(complex.id);
                  },
                }}
              >
                {isSelected ? (
                  <Tooltip key={`${complex.id}-selected`} direction="top" offset={[0, -10]} permanent>
                    {complex.name}
                  </Tooltip>
                ) : (
                  <Tooltip key={`${complex.id}-hover`} direction="top" offset={[0, -10]}>
                    {complex.name}
                  </Tooltip>
                )}
                <Popup>
                  <div className="atlas-map-popup">
                    <strong>{complex.name}</strong>
                    <span>
                      {complex.address}, {complex.city}
                    </span>
                    <span>Trust score: {complex.trustScore.toFixed(1)} / 10</span>
                    <span>
                      {commuteTime} min {transportMode} to {campusBuilding.name}
                    </span>
                  </div>
                </Popup>
              </CircleMarker>
            );
          })}
        </MapContainer>
      </div>
    </div>
  );
}
