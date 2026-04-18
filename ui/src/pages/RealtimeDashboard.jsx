import { useEffect, useMemo, useState } from "react";
import PageContainer from "../components/layout/PageContainer";
import StatCard from "../components/cards/StatCard";
import ProvinceFilter from "../components/filters/ProvinceFilter";
import MetricFilter from "../components/filters/MetricFilter";
import RealtimeLineChart from "../components/charts/RealtimeLineChart";
import SimpleBarChart from "../components/charts/SimpleBarChart";
import {
  POLLUTION_METRIC_OPTIONS,
  WEATHER_METRIC_OPTIONS,
} from "../utils/constants";
import { getProvinceName } from "../utils/provinceMap";
import {
  getRealtimeOpenAQ,
  getRealtimeWeather,
} from "../services/api";

function RealtimeDashboard() {
  const [openaqOverview, setOpenaqOverview] = useState(null);
  const [weatherOverview, setWeatherOverview] = useState(null);
  const [province, setProvince] = useState("ALL");
  const [pollutionMetric, setPollutionMetric] = useState("pm25");
  const [weatherMetric, setWeatherMetric] = useState("temp");

  useEffect(() => {
    async function load() {
      try {
        const [openaqData, weatherData] = await Promise.all([
          getRealtimeOpenAQ(),
          getRealtimeWeather(),
        ]);
        setOpenaqOverview(openaqData);
        setWeatherOverview(weatherData);
      } catch (error) {
        console.error(error);
      }
    }

    load();
    const interval = setInterval(load, 30000);
    return () => clearInterval(interval);
  }, []);

  const provinceOptions = useMemo(() => {
    const provinces = openaqOverview?.provinces?.map((p) => p.province) || [];
    return ["ALL", ...provinces];
  }, [openaqOverview]);

  const pollutionBarData = useMemo(() => {
    if (!openaqOverview?.provinces) return [];
    return openaqOverview.provinces.map((p) => ({
      label: p.province,
      value: p[pollutionMetric],
    }));
  }, [openaqOverview, pollutionMetric]);

  const weatherBarData = useMemo(() => {
    if (!weatherOverview?.provinces) return [];
    return weatherOverview.provinces.map((p) => ({
      label: p.province,
      value: p[weatherMetric],
    }));
  }, [weatherOverview, weatherMetric]);

  const selectedPollutionSeries = useMemo(() => {
    if (province === "ALL") return [];
    return openaqOverview?.series?.[province] || [];
  }, [province, openaqOverview]);

  const selectedWeatherSeries = useMemo(() => {
    if (province === "ALL") return [];
    return weatherOverview?.series?.[province] || [];
  }, [province, weatherOverview]);

  const selectedPollutionLatest = useMemo(() => {
    if (province === "ALL") return null;
    const rows = openaqOverview?.series?.[province] || [];
    return rows[rows.length - 1] || null;
  }, [province, openaqOverview]);

  const selectedWeatherLatest = useMemo(() => {
    if (province === "ALL") return null;
    const rows = weatherOverview?.series?.[province] || [];
    return rows[rows.length - 1] || null;
  }, [province, weatherOverview]);

  const lastUpdate =
    openaqOverview?.timestamp || weatherOverview?.timestamp || "Loading...";

  return (
    <PageContainer
      title="Realtime Air Pollution Dashboard"
      subtitle={`Cập nhật gần nhất: ${lastUpdate}`}
    >
      <div className="panel">
        <div className="filter-row">
          <ProvinceFilter
            value={province}
            onChange={setProvince}
            options={provinceOptions}
          />
          <MetricFilter
            value={pollutionMetric}
            onChange={setPollutionMetric}
            options={POLLUTION_METRIC_OPTIONS}
          />
          <MetricFilter
            value={weatherMetric}
            onChange={setWeatherMetric}
            options={WEATHER_METRIC_OPTIONS}
          />
        </div>
      </div>

      {province === "ALL" ? (
        <>
          <div className="card-grid">
            <StatCard
              title="PM2.5 trung bình"
              value={openaqOverview?.summary?.avg_pm25 ?? "--"}
            />
            <StatCard
              title="AQI trung bình"
              value={openaqOverview?.summary?.avg_aqi ?? "--"}
            />
            <StatCard
              title="Khu vực ô nhiễm nhất"
              value={openaqOverview?.summary?.worst_region
                ? getProvinceName(openaqOverview.summary.worst_region)
                : "--"}
            />
            <StatCard
              title="Số khu vực cảnh báo"
              value={openaqOverview?.summary?.alert_regions ?? "--"}
            />
          </div>

          <div className="panel">
            <h3>Tổng hợp ô nhiễm theo tỉnh/thành</h3>
            <SimpleBarChart
              data={pollutionBarData}
              title={`${pollutionMetric.toUpperCase()} hiện tại theo tỉnh`}
            />
          </div>

          <div className="panel">
            <h3>Tổng hợp thời tiết theo tỉnh/thành</h3>
            <SimpleBarChart
              data={weatherBarData}
              title={`${weatherMetric} hiện tại theo tỉnh`}
            />
          </div>
        </>
      ) : (
        <>
          <div className="card-grid">
            <StatCard title="PM2.5" value={selectedPollutionLatest?.pm25 ?? "--"} />
            <StatCard title="PM10" value={selectedPollutionLatest?.pm10 ?? "--"} />
            <StatCard title="NO2" value={selectedPollutionLatest?.no2 ?? "--"} />
            <StatCard title="AQI" value={selectedPollutionLatest?.aqi ?? "--"} />
          </div>

          <div className="card-grid">
            <StatCard
              title="Nhiệt độ"
              value={selectedWeatherLatest?.temp ?? "--"}
              unit=" °C"
            />
            <StatCard
              title="Độ ẩm"
              value={selectedWeatherLatest?.humidity ?? "--"}
              unit=" %"
            />
            <StatCard
              title="Gió"
              value={selectedWeatherLatest?.wind ?? "--"}
              unit=" kph"
            />
            <StatCard
              title="Áp suất"
              value={selectedWeatherLatest?.pressure ?? "--"}
              unit=" mb"
            />
          </div>

          <div className="panel">
            <h3>Xu hướng ô nhiễm tại {getProvinceName(province)}</h3>
            <RealtimeLineChart
              data={selectedPollutionSeries}
              metric={pollutionMetric}
              title={`${pollutionMetric.toUpperCase()} theo thời gian`}
              color="#f97316"
            />
          </div>

          <div className="panel">
            <h3>Xu hướng thời tiết tại {getProvinceName(province)}</h3>
            <RealtimeLineChart
              data={selectedWeatherSeries}
              metric={weatherMetric}
              title={`${weatherMetric} theo thời gian`}
              color="#38bdf8"
            />
          </div>
        </>
      )}
    </PageContainer>
  );
}

export default RealtimeDashboard;