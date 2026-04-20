export async function getRealtimeOpenAQ() {
  const response = await fetch("/mock/realtime-openaq.json");
  if (!response.ok) throw new Error("Failed to load realtime OpenAQ");
  return response.json();
}

export async function getRealtimeWeather() {
  const response = await fetch("/mock/realtime-weather.json");
  if (!response.ok) throw new Error("Failed to load realtime weather");
  return response.json();
}

export async function getHistoricalOpenAQ() {
  const response = await fetch("/mock/historical-openaq.json");
  if (!response.ok) throw new Error("Failed to load historical OpenAQ");
  return response.json();
}

export async function getHistoricalWeather() {
  const response = await fetch("/mock/historical-weather.json");
  if (!response.ok) throw new Error("Failed to load historical weather");
  return response.json();
}

export async function getHistoricalSentinel() {
  const response = await fetch("/mock/historical-sentinel.json");
  if (!response.ok) throw new Error("Failed to load historical Sentinel-5P");
  return response.json();
}