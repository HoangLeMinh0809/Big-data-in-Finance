import { useEffect, useMemo, useState } from "react";
import PageContainer from "../components/layout/PageContainer";
import HistoricalLineChart from "../components/charts/HistoricalLineChart";
import SimpleBarChart from "../components/charts/SimpleBarChart";
import ProvinceFilter from "../components/filters/ProvinceFilter";
import MetricFilter from "../components/filters/MetricFilter";
import DateRangeFilter from "../components/filters/DateRangeFilter";
import {
  POLLUTION_METRIC_OPTIONS,
  WEATHER_METRIC_OPTIONS,
} from "../utils/constants";
import { getProvinceName } from "../utils/provinceMap";
import {
  getHistoricalOpenAQ,
  getHistoricalWeather,
  getHistoricalSentinel,
} from "../services/api";

function HistoricalDashboard() {
  const [tab, setTab] = useState("openaq");
  const [openaqRows, setOpenaqRows] = useState([]);
  const [weatherRows, setWeatherRows] = useState([]);
  const [sentinelRows, setSentinelRows] = useState([]);
  const [province, setProvince] = useState("ALL");
  const [metric, setMetric] = useState("pm25");
  const [startDate, setStartDate] = useState("2024-12-28");
  const [endDate, setEndDate] = useState("2025-01-02");

  useEffect(() => {
    async function load() {
      try {
        const [openaqData, weatherData, sentinelData] = await Promise.all([
          getHistoricalOpenAQ(),
          getHistoricalWeather(),
          getHistoricalSentinel(),
        ]);
        setOpenaqRows(openaqData);
        setWeatherRows(weatherData);
        setSentinelRows(sentinelData);
      } catch (error) {
        console.error(error);
      }
    }
    load();
  }, []);

  const sourceOptions = [
    { value: "openaq", label: "OpenAQ", badge: "Ô nhiễm không khí" },
    { value: "weather", label: "Weather", badge: "Thời tiết lịch sử" },
    { value: "sentinel", label: "Sentinel-5P", badge: "Dữ liệu vệ tinh" },
  ];

  const selectedSource = sourceOptions.find((item) => item.value === tab);

  const provinceOptions = useMemo(() => {
    const baseRows = tab === "weather" ? weatherRows : openaqRows;
    const unique = [...new Set(baseRows.map((r) => r.province))];
    return ["ALL", ...unique];
  }, [tab, openaqRows, weatherRows]);

  const currentRows = useMemo(() => {
    if (tab === "weather") return weatherRows;
    if (tab === "openaq") return openaqRows;
    return [];
  }, [tab, openaqRows, weatherRows]);

  const filteredRows = useMemo(() => {
    return currentRows.filter((row) => {
      const date = row.timestamp.slice(0, 10);
      const matchProvince = province === "ALL" ? true : row.province === province;
      return matchProvince && date >= startDate && date <= endDate;
    });
  }, [currentRows, province, startDate, endDate]);

  const barData = useMemo(() => {
    if (province !== "ALL") return [];
    const grouped = new Map();

    filteredRows.forEach((row) => {
      const value = Number(row[metric]);
      if (Number.isNaN(value)) return;

      if (!grouped.has(row.province)) grouped.set(row.province, []);
      grouped.get(row.province).push(value);
    });

    return Array.from(grouped.entries()).map(([label, values]) => ({
      label,
      value: values.reduce((a, b) => a + b, 0) / values.length,
    }));
  }, [filteredRows, province, metric]);

  const weatherMode = tab === "weather";
  const metricOptions = weatherMode
    ? WEATHER_METRIC_OPTIONS
    : POLLUTION_METRIC_OPTIONS;

  return (
    <PageContainer
      title="Historical Dashboard"
      subtitle="Phân tích lịch sử ô nhiễm, thời tiết và Sentinel-5P"
    >
      <div className="dashboard-section source-panel">
        <div className="section-head">
          <div>
            <h3 className="section-title">Nguồn dữ liệu</h3>
            <p className="section-subtitle">
              Chọn bộ dữ liệu bạn muốn phân tích
            </p>
          </div>
        </div>

        <div className="source-select-wrap">
          <div className="source-select-box">
            <label className="input-label">Dataset</label>
            <select
              value={tab}
              onChange={(e) => {
                const value = e.target.value;
                setTab(value);

                if (value === "openaq") setMetric("pm25");
                else if (value === "weather") setMetric("temp");
              }}
              className="source-select"
            >
              <option value="openaq">OpenAQ</option>
              <option value="weather">Weather</option>
              <option value="sentinel">Sentinel-5P</option>
            </select>
          </div>

          <div className="source-chip">
            <span className="source-chip-dot" />
            {selectedSource?.badge}
          </div>
        </div>
      </div>

      {tab !== "sentinel" ? (
        <>
          <div className="dashboard-section filter-panel">
            <div className="section-head">
              <div>
                <h3 className="section-title">Bộ lọc phân tích</h3>
                <p className="section-subtitle">
                  Lọc theo tỉnh/thành, chỉ số và khoảng thời gian
                </p>
              </div>
            </div>

            <div className="filter-row enhanced-filter-row">
              <ProvinceFilter
                value={province}
                onChange={setProvince}
                options={provinceOptions}
              />
              <MetricFilter
                value={metric}
                onChange={setMetric}
                options={metricOptions}
              />
              <DateRangeFilter
                startDate={startDate}
                endDate={endDate}
                onStartChange={setStartDate}
                onEndChange={setEndDate}
              />
            </div>
          </div>

          {province === "ALL" ? (
            <div className="dashboard-section chart-panel">
              <div className="section-head">
                <div>
                  <h3 className="section-title">Trung bình theo tỉnh/thành</h3>
                  <p className="section-subtitle">
                    So sánh giá trị trung bình giữa các tỉnh trong khoảng thời gian đã chọn
                  </p>
                </div>
              </div>

              <SimpleBarChart
                data={barData}
                title={`Giá trị trung bình ${metric} theo tỉnh`}
              />
            </div>
          ) : (
            <div className="dashboard-section chart-panel">
              <div className="section-head">
                <div>
                  <h3 className="section-title">
                    Xu hướng lịch sử tại {getProvinceName(province)}
                  </h3>
                  <p className="section-subtitle">
                    Theo dõi biến động của chỉ số {metric} theo thời gian
                  </p>
                </div>
              </div>

              <HistoricalLineChart
                data={filteredRows}
                metric={metric}
                title={`${metric} tại ${getProvinceName(province)}`}
              />
            </div>
          )}
        </>
      ) : (
        <div className="dashboard-section chart-panel">
          <div className="section-head">
            <div>
              <h3 className="section-title">Sentinel-5P historical layers</h3>
              <p className="section-subtitle">
                Danh sách lớp dữ liệu vệ tinh theo thời gian công bố
              </p>
            </div>
          </div>

          <div className="sentinel-table-wrap">
            <table className="sentinel-table">
              <thead>
                <tr>
                  <th>Product</th>
                  <th>Start Time</th>
                  <th>Publication</th>
                </tr>
              </thead>
              <tbody>
                {sentinelRows.map((row) => (
                  <tr key={row.id}>
                    <td>{row.product_type}</td>
                    <td>{row.start_time_utc}</td>
                    <td>{row.publication_date}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </PageContainer>
  );
}

export default HistoricalDashboard;