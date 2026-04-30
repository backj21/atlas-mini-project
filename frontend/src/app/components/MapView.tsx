import { useEffect, useMemo, useState } from 'react';
import { divIcon } from 'leaflet';
import type { LatLngBoundsExpression, LatLngExpression } from 'leaflet';
import { CircleMarker, MapContainer, Marker, Polyline, Popup, TileLayer, Tooltip, useMap } from 'react-leaflet';

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
  const walkableSchoolIcon = useMemo(
    () =>
      divIcon({
        className: 'atlas-map-school-div-icon atlas-map-school-div-icon-walk',
        html: '<span class="atlas-map-school-ping"></span><span class="atlas-map-school-dot"></span>',
        iconAnchor: [16, 16],
        iconSize: [32, 32],
      }),
    [],
  );
  const bikeableSchoolIcon = useMemo(
    () =>
      divIcon({
        className: 'atlas-map-school-div-icon atlas-map-school-div-icon-bike',
        html: '<span class="atlas-map-school-ping"></span><span class="atlas-map-school-dot"></span>',
        iconAnchor: [16, 16],
        iconSize: [32, 32],
      }),
    [],
  );
  const focusedApartmentIcon = useMemo(
    () =>
      divIcon({
        className: 'atlas-map-focused-apartment-div-icon',
        html: [
          '<span class="atlas-map-apartment-ring"></span>',
          '<span class="atlas-map-apartment-ring"></span>',
          '<span class="atlas-map-apartment-core"></span>',
        ].join(''),
        iconAnchor: [34, 34],
        iconSize: [68, 68],
      }),
    [],
  );
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
        className={isFocused ? 'atlas-map-shell atlas-map-shell-focused' : 'atlas-map-shell'}
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
            ? focusedSchoolBuildings.map((building) => (
                <Polyline
                  key={`${focusedComplexId}-${building.building}-line`}
                  positions={[
                    [activeComplex.lat, activeComplex.lng],
                    [building.lat, building.lng],
                  ]}
                  pathOptions={{
                    color: building.isWalkable ? 'var(--green)' : 'var(--blue)',
                    className: 'atlas-map-route-line',
                    dashArray: '4 7',
                    opacity: 0.42,
                    weight: 1.5,
                  }}
                />
              ))
            : null}

          {isFocused && activeComplex ? (
            <Marker
              key={`${focusedComplexId}-focused-apartment`}
              position={[activeComplex.lat, activeComplex.lng]}
              icon={focusedApartmentIcon}
              zIndexOffset={380}
            >
              <Tooltip direction="top" offset={[0, -24]} permanent>
                {activeComplex.name}
              </Tooltip>
              <Popup>
                <div className="atlas-map-popup">
                  <strong>{activeComplex.name}</strong>
                  <span>
                    {activeComplex.address}, {activeComplex.city}
                  </span>
                  <span>Trust score: {activeComplex.trustScore.toFixed(1)} / 10</span>
                </div>
              </Popup>
            </Marker>
          ) : null}

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
              <Marker
                key={`${focusedComplexId}-nearby-${building.building}`}
                position={[building.lat, building.lng]}
                icon={building.isWalkable ? walkableSchoolIcon : bikeableSchoolIcon}
                zIndexOffset={320}
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
              </Marker>
            );
          })}

          {!isFocused ? complexes.map((complex) => {
            const isSelected = complex.id === selectedComplexId;
            const commuteTime = transportMode === 'walk' ? complex.walkTime : complex.bikeTime;

            return (
              <CircleMarker
                key={complex.id}
                center={[complex.lat, complex.lng]}
                radius={isSelected ? 10 : 6}
                pathOptions={{
                  color: isSelected ? 'var(--orange)' : 'var(--paper)',
                  className: 'atlas-map-apartment-marker',
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
          }) : null}
        </MapContainer>
      </div>
    </div>
  );
}
