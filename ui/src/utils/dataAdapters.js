export function adaptWeatherApiDay(weatherJson) {
  const location = weatherJson.location;
  const forecastDay = weatherJson.forecast?.forecastday?.[0];
  const hours = forecastDay?.hour || [];

  return hours.map((hour) => ({
    source: "weather",
    region: location?.name || "Unknown",
    country: location?.country || "Unknown",
    lat: location?.lat ?? null,
    lon: location?.lon ?? null,
    timestamp: hour.time,
    temp: hour.temp_c ?? null,
    humidity: hour.humidity ?? null,
    wind: hour.wind_kph ?? null,
    pressure: hour.pressure_mb ?? null,
    uv: hour.uv ?? null,
    cloud: hour.cloud ?? null,
    aqi: null,
    pm25: null,
    no2: null,
  }));
}

export function adaptOpenAQRows(rows) {
  return rows.map((row) => ({
    source: "openaq",
    region: row.location || row.city || "Unknown",
    country: row.country || "Unknown",
    lat: row.coordinates?.latitude ?? null,
    lon: row.coordinates?.longitude ?? null,
    timestamp: row.timestamp || row.datetime || row.date?.utc,
    temp: null,
    humidity: null,
    wind: null,
    pressure: null,
    uv: null,
    cloud: null,
    aqi: row.aqi ?? null,
    pm25: row.pm25 ?? row.value ?? null,
    no2: row.no2 ?? null,
  }));
}

export function mergeNormalizedData(...groups) {
  return groups.flat().filter((item) => item.timestamp);
}

export function getLatestRecord(data) {
  if (!data.length) return null;
  return [...data].sort(
    (a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
  )[0];
}

export function buildRegionAverages(data, metric) {
  const grouped = new Map();

  data.forEach((item) => {
    const key = item.region || "Unknown";
    const value = Number(item[metric]);
    if (Number.isNaN(value)) return;

    if (!grouped.has(key)) {
      grouped.set(key, []);
    }
    grouped.get(key).push(value);
  });

  return Array.from(grouped.entries()).map(([label, values]) => ({
    label,
    value: values.reduce((sum, item) => sum + item, 0) / values.length,
  }));
}