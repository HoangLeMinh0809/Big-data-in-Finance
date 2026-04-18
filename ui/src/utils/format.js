export function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }
  return Number(value).toFixed(1);
}

export function formatDateTime(value) {
  return new Date(value).toLocaleString("vi-VN");
}