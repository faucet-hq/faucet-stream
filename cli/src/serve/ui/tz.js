// UI timezone preference. Data is ALWAYS UTC on the wire — this module only
// controls how timestamps are *displayed* and how the date picker interprets an
// entered wall-clock time. Default is the browser's local zone. Persisted in
// localStorage so it survives reloads (like the theme toggle).

const KEY = "faucet-tz";

export function getTz() {
  try { return localStorage.getItem(KEY) || "local"; } catch { return "local"; }
}
export function setTz(z) {
  try { localStorage.setItem(KEY, z); } catch { /* ignore */ }
}

const LOCAL_ZONE = (() => {
  try { return Intl.DateTimeFormat().resolvedOptions().timeZone || ""; } catch { return ""; }
})();

/** Curated zone list for the selector (Local first, then UTC, then a spread). */
export const TZ_OPTIONS = [
  { value: "local", label: `Local${LOCAL_ZONE ? ` - ${LOCAL_ZONE}` : ""}` },
  { value: "UTC", label: "UTC" },
  { value: "America/Los_Angeles", label: "Los Angeles" },
  { value: "America/New_York", label: "New York" },
  { value: "Europe/London", label: "London" },
  { value: "Europe/Berlin", label: "Berlin" },
  { value: "Asia/Kolkata", label: "Kolkata" },
  { value: "Asia/Singapore", label: "Singapore" },
  { value: "Asia/Tokyo", label: "Tokyo" },
  { value: "Australia/Sydney", label: "Sydney" },
];

/** Short label for inline hints (e.g. the picker's TIME row). */
export function tzShort() {
  const tz = getTz();
  return tz === "local" ? "local" : tz;
}

const zoneOpt = (tz) => (tz === "local" ? {} : { timeZone: tz });

/** Format a UTC/ISO value for display in the selected zone. "—" if empty/invalid. */
export function formatTs(value) {
  if (!value) return "—";
  const d = new Date(value);
  if (isNaN(d.getTime())) return "—";
  try {
    return new Intl.DateTimeFormat("en-US", {
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: true,
      ...zoneOpt(getTz()),
    }).format(d);
  } catch {
    return d.toLocaleString();
  }
}

/** `value` split into a time and a date, both in the display timezone, for
 *  layouts that show the time prominently and the date beneath it. */
export function formatTsSplit(value) {
  const d = value ? new Date(value) : null;
  if (!d || isNaN(d.getTime())) return null;
  const fmt = (opts) => {
    try {
      return new Intl.DateTimeFormat("en-US", { ...opts, ...zoneOpt(getTz()) }).format(d);
    } catch {
      return new Intl.DateTimeFormat("en-US", opts).format(d);
    }
  };
  return {
    time: fmt({ hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: true }),
    date: fmt({ year: "numeric", month: "short", day: "numeric" }),
  };
}

function partsInZone(date, tz, withSeconds) {
  const p = new Intl.DateTimeFormat("en-US", {
    timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", ...(withSeconds ? { second: "2-digit" } : {}),
    hour12: false,
  }).formatToParts(date).reduce((a, x) => { a[x.type] = x.value; return a; }, {});
  return p;
}

/**
 * Interpret a wall-clock (fields as the user typed them, in the selected zone)
 * as a real UTC instant. For "local" this is just `new Date(...)`; for a named
 * zone it computes that zone's offset at the instant and corrects for it.
 */
export function zonedWallToUtc(y, mo, d, h, mi) {
  const tz = getTz();
  if (tz === "local") return new Date(y, mo, d, h, mi);
  const asUtc = Date.UTC(y, mo, d, h, mi);
  const p = partsInZone(new Date(asUtc), tz, true);
  const shown = Date.UTC(+p.year, +p.month - 1, +p.day, +p.hour % 24, +p.minute, +p.second);
  return new Date(asUtc - (shown - asUtc)); // subtract the zone's offset
}

/** UTC Date → the wall-clock fields it shows as in the selected zone (for reopening). */
export function utcToZonedWall(date) {
  const tz = getTz();
  if (tz === "local") {
    return { y: date.getFullYear(), mo: date.getMonth(), d: date.getDate(), h: date.getHours(), mi: date.getMinutes() };
  }
  const p = partsInZone(date, tz, false);
  return { y: +p.year, mo: +p.month - 1, d: +p.day, h: +p.hour % 24, mi: +p.minute };
}
