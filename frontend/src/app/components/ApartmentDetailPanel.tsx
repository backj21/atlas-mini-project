import apartmentDetailsData from '../data/apartmentDetails.json';

interface ApartmentComplex {
  id: string;
  name: string;
  address: string;
  city: string;
  trustScore: number;
}

interface AmenityValue {
  label: string;
  available: boolean;
}

interface ScoreSummary {
  amenityScore?: number | null;
  petScore?: number | null;
  priceScore?: number | null;
  totalScore?: number | null;
}

interface PricingPlan {
  beds: number | null;
  baths: number | null;
  sqft: number | null;
  rent: number;
  perPerson: boolean;
  notes: string | null;
}

interface PricingSummary {
  minRent: number | null;
  maxRent: number | null;
  floorPlanCount: number;
  perPerson: boolean;
  samplePlans: PricingPlan[];
}

interface CommuteSummary {
  building: string;
  walkingMin: number | null;
  bicyclingMin: number | null;
  transitMin: number | null;
}

interface RedditHighlight {
  text: string;
  author: string;
  url: string | null;
  score: number;
  postTitle: string | null;
}

interface GoogleHighlight {
  text: string;
  author: string;
  rating: number;
  date: string | null;
  likesCount: number;
  url: string | null;
}

interface ApartmentDetails {
  amenities: Record<string, AmenityValue>;
  scores: ScoreSummary;
  pricing: PricingSummary;
  commute: CommuteSummary[];
  reviews: {
    reddit: {
      positive: RedditHighlight | null;
      negative: RedditHighlight | null;
    };
    google: {
      positive: GoogleHighlight | null;
      negative: GoogleHighlight | null;
    };
  };
}

interface ApartmentDetailPanelProps {
  apartment: ApartmentComplex;
  selectedBuildingName: string;
}

const apartmentDetails = apartmentDetailsData as Record<string, ApartmentDetails>;

const moneyFormatter = new Intl.NumberFormat('en-US', {
  currency: 'USD',
  maximumFractionDigits: 0,
  style: 'currency',
});

const dateFormatter = new Intl.DateTimeFormat('en-US', {
  month: 'short',
  day: 'numeric',
  year: 'numeric',
});

function sectionLabel(children: string) {
  return (
    <div
      style={{
        color: 'var(--ink-3)',
        fontFamily: 'var(--mono)',
        fontSize: '10.5px',
        letterSpacing: '0.08em',
        marginBottom: '12px',
        textTransform: 'uppercase',
      }}
    >
      {children}
    </div>
  );
}

function formatRent(value: number | null) {
  return value === null ? 'N/A' : moneyFormatter.format(value);
}

function formatDate(value: string | null) {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return dateFormatter.format(date);
}

function formatScore(value: number | null | undefined) {
  if (value === null || value === undefined) return 'N/A';
  return (value * 10).toFixed(1);
}

function formatCompactNumber(value: number) {
  return Number.isInteger(value) ? String(value) : value.toFixed(1);
}

function formatPlan(plan: PricingPlan) {
  const beds = plan.beds === 0 ? 'Studio' : `${plan.beds === null ? 'N/A' : formatCompactNumber(plan.beds)} bed`;
  const baths = plan.baths === null ? '' : ` / ${formatCompactNumber(plan.baths)} bath`;
  const sqft = plan.sqft === null ? '' : ` · ${plan.sqft.toLocaleString()} sqft`;
  return `${beds}${baths}${sqft}`;
}

function ReviewBlock({
  source,
  tone,
  highlight,
}: {
  source: 'Google Maps' | 'Reddit';
  tone: 'positive' | 'negative';
  highlight: GoogleHighlight | RedditHighlight | null;
}) {
  const toneLabel = tone === 'positive' ? 'Good' : 'Bad';
  const toneColor = tone === 'positive' ? 'var(--green)' : 'var(--red)';

  return (
    <div
      style={{
        borderTop: '1px solid var(--rule-2)',
        paddingTop: '12px',
      }}
    >
      <div
        className="flex items-center justify-between"
        style={{
          gap: '12px',
          marginBottom: '7px',
        }}
      >
        <span
          style={{
            color: toneColor,
            fontFamily: 'var(--mono)',
            fontSize: '10.5px',
            letterSpacing: '0.08em',
            textTransform: 'uppercase',
          }}
        >
          {source} · {toneLabel}
        </span>
        {highlight && 'rating' in highlight ? (
          <span
            style={{
              color: 'var(--ink)',
              fontFamily: 'var(--mono)',
              fontSize: '11px',
            }}
          >
            {highlight.rating.toFixed(1)} stars
          </span>
        ) : null}
      </div>

      {highlight ? (
        <>
          <p
            style={{
              color: 'var(--ink-2)',
              fontFamily: 'var(--sans)',
              fontSize: '13px',
              lineHeight: 1.55,
              margin: 0,
            }}
          >
            "{highlight.text}"
          </p>
          <div
            className="flex items-center justify-between"
            style={{
              color: 'var(--ink-3)',
              fontFamily: 'var(--mono)',
              fontSize: '10.5px',
              gap: '12px',
              marginTop: '9px',
            }}
          >
            <span>
              {highlight.author}
              {'date' in highlight && formatDate(highlight.date) ? ` · ${formatDate(highlight.date)}` : ''}
            </span>
            {highlight.url ? (
              <a
                href={highlight.url}
                rel="noreferrer"
                target="_blank"
                style={{
                  color: 'var(--orange)',
                  textDecoration: 'none',
                }}
              >
                Source
              </a>
            ) : null}
          </div>
        </>
      ) : (
        <p
          style={{
            color: 'var(--ink-3)',
            fontFamily: 'var(--mono)',
            fontSize: '11px',
            lineHeight: 1.55,
            margin: 0,
          }}
        >
          Not enough curated {source} data for a meaningful {toneLabel.toLowerCase()} highlight.
        </p>
      )}
    </div>
  );
}

export function ApartmentDetailPanel({ apartment, selectedBuildingName }: ApartmentDetailPanelProps) {
  const details = apartmentDetails[apartment.id];

  if (!details) {
    return null;
  }

  const selectedCommute = details.commute.find((item) => item.building === selectedBuildingName);
  const alternateCommutes = details.commute
    .filter((item) => item.building !== selectedBuildingName)
    .slice()
    .sort((a, b) => (a.walkingMin ?? 999) - (b.walkingMin ?? 999))
    .slice(0, 3);

  const amenities = Object.values(details.amenities);
  const includedAmenities = amenities.filter((amenity) => amenity.available);
  const unavailableAmenities = amenities.filter((amenity) => !amenity.available);
  const pricing = details.pricing;
  const rentRange =
    pricing.minRent === pricing.maxRent
      ? formatRent(pricing.minRent)
      : `${formatRent(pricing.minRent)}-${formatRent(pricing.maxRent)}`;

  return (
    <aside
      className="self-start"
      style={{
        background: 'var(--paper)',
        border: '1px solid var(--rule)',
        maxHeight: 'calc(100vh - 96px)',
        overflowY: 'auto',
        padding: '20px',
        position: 'sticky',
        top: '24px',
      }}
    >
      <div
        style={{
          borderBottom: '1px solid var(--rule)',
          paddingBottom: '16px',
        }}
      >
        <div
          style={{
            color: 'var(--ink-3)',
            fontFamily: 'var(--mono)',
            fontSize: '10.5px',
            letterSpacing: '0.08em',
            marginBottom: '9px',
            textTransform: 'uppercase',
          }}
        >
          Selected apartment
        </div>
        <h3
          style={{
            color: 'var(--ink)',
            fontFamily: 'var(--serif)',
            fontSize: '25px',
            fontWeight: 500,
            letterSpacing: '-0.01em',
            lineHeight: 1.15,
            margin: 0,
          }}
        >
          {apartment.name}
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
          {apartment.address}, {apartment.city}
        </div>
      </div>

      <section style={{ borderBottom: '1px solid var(--rule)', padding: '16px 0' }}>
        {sectionLabel('Snapshot')}
        <div className="grid grid-cols-3" style={{ gap: '14px' }}>
          <Metric label="Trust" value={apartment.trustScore.toFixed(1)} />
          <Metric label="Score" value={formatScore(details.scores.totalScore)} />
          <Metric label="Plans" value={pricing.floorPlanCount ? String(pricing.floorPlanCount) : 'N/A'} />
        </div>
      </section>

      <section style={{ borderBottom: '1px solid var(--rule)', padding: '16px 0' }}>
        {sectionLabel(`Commute to ${selectedBuildingName}`)}
        {selectedCommute ? (
          <div className="grid grid-cols-3" style={{ gap: '14px', marginBottom: '14px' }}>
            <Metric label="Walk" value={`${selectedCommute.walkingMin ?? 'N/A'}m`} />
            <Metric label="Bike" value={`${selectedCommute.bicyclingMin ?? 'N/A'}m`} />
            <Metric label="Transit" value={`${selectedCommute.transitMin ?? 'N/A'}m`} />
          </div>
        ) : (
          <p style={{ color: 'var(--ink-3)', fontFamily: 'var(--mono)', fontSize: '11px', margin: 0 }}>
            No curated commute row for this destination.
          </p>
        )}

        <div className="grid" style={{ gap: '7px' }}>
          {alternateCommutes.map((commute) => (
            <div
              key={commute.building}
              className="flex items-center justify-between"
              style={{
                color: 'var(--ink-3)',
                fontFamily: 'var(--mono)',
                fontSize: '10.5px',
                gap: '12px',
              }}
            >
              <span>{commute.building}</span>
              <span style={{ color: 'var(--ink)' }}>{commute.walkingMin ?? 'N/A'} min walk</span>
            </div>
          ))}
        </div>
      </section>

      <section style={{ borderBottom: '1px solid var(--rule)', padding: '16px 0' }}>
        {sectionLabel('Pricing')}
        <div
          style={{
            color: 'var(--ink)',
            fontFamily: 'var(--serif)',
            fontSize: '24px',
            fontWeight: 500,
            lineHeight: 1,
            marginBottom: '6px',
          }}
        >
          {rentRange}
        </div>
        <div
          style={{
            color: 'var(--ink-3)',
            fontFamily: 'var(--mono)',
            fontSize: '10.5px',
            marginBottom: '14px',
          }}
        >
          {pricing.perPerson ? 'Listed per person' : 'Listed per unit'} · {pricing.floorPlanCount || 'No'} curated plans
        </div>
        <div className="grid" style={{ gap: '8px' }}>
          {pricing.samplePlans.map((plan) => (
            <div
              key={`${plan.notes}-${plan.rent}-${plan.sqft}`}
              className="flex items-center justify-between"
              style={{
                borderTop: '1px solid var(--rule-2)',
                color: 'var(--ink-2)',
                fontFamily: 'var(--mono)',
                fontSize: '10.5px',
                gap: '10px',
                paddingTop: '8px',
              }}
            >
              <span>{plan.notes || formatPlan(plan)}</span>
              <span style={{ color: 'var(--ink)' }}>{moneyFormatter.format(plan.rent)}</span>
            </div>
          ))}
        </div>
      </section>

      <section style={{ borderBottom: '1px solid var(--rule)', padding: '16px 0' }}>
        {sectionLabel('Amenities')}
        <div className="flex flex-wrap" style={{ gap: '7px' }}>
          {includedAmenities.map((amenity) => (
            <AmenityBadge key={amenity.label} available label={amenity.label} />
          ))}
          {unavailableAmenities.map((amenity) => (
            <AmenityBadge key={amenity.label} available={false} label={amenity.label} />
          ))}
        </div>
      </section>

      <section style={{ paddingTop: '16px' }}>
        {sectionLabel('Review highlights')}
        <div className="grid" style={{ gap: '16px' }}>
          <ReviewBlock source="Reddit" tone="positive" highlight={details.reviews.reddit.positive} />
          <ReviewBlock source="Reddit" tone="negative" highlight={details.reviews.reddit.negative} />
          <ReviewBlock source="Google Maps" tone="positive" highlight={details.reviews.google.positive} />
          <ReviewBlock source="Google Maps" tone="negative" highlight={details.reviews.google.negative} />
        </div>
      </section>
    </aside>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div
        style={{
          color: 'var(--ink)',
          fontFamily: 'var(--serif)',
          fontSize: '24px',
          fontWeight: 500,
          lineHeight: 1,
        }}
      >
        {value}
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
        {label}
      </div>
    </div>
  );
}

function AmenityBadge({ available, label }: { available: boolean; label: string }) {
  return (
    <span
      style={{
        background: available ? 'var(--cream)' : 'transparent',
        border: `1px solid ${available ? 'var(--rule)' : 'var(--rule-2)'}`,
        color: available ? 'var(--ink)' : 'var(--ink-4)',
        fontFamily: 'var(--mono)',
        fontSize: '10.5px',
        padding: '5px 7px',
      }}
    >
      {available ? '+ ' : '- '}
      {label}
    </span>
  );
}
