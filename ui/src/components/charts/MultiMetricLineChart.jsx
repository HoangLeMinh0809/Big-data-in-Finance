import { useEffect, useRef } from "react";
import * as d3 from "d3";

const SERIES = [
  { key: "temp", label: "Temperature", color: "#f97316" },
  { key: "humidity", label: "Humidity", color: "#38bdf8" },
  { key: "wind", label: "Wind", color: "#22c55e" },
];

function MultiMetricLineChart({ data, title }) {
  const ref = useRef();

  useEffect(() => {
    const container = d3.select(ref.current);
    container.selectAll("*").remove();

    const width = 960;
    const height = 380;
    const margin = { top: 24, right: 30, bottom: 40, left: 60 };

    const svg = container
      .append("svg")
      .attr("width", width)
      .attr("height", height);

    const parsed = data.map((d) => ({
      ...d,
      date: new Date(d.timestamp),
    }));

    const x = d3
      .scaleTime()
      .domain(d3.extent(parsed, (d) => d.date))
      .range([margin.left, width - margin.right]);

    const allValues = parsed.flatMap((d) => [d.temp, d.humidity, d.wind].map(Number));

    const y = d3
      .scaleLinear()
      .domain([d3.min(allValues) - 2, d3.max(allValues) + 2])
      .nice()
      .range([height - margin.bottom, margin.top]);

    svg
      .append("g")
      .attr("transform", `translate(0,${height - margin.bottom})`)
      .call(d3.axisBottom(x).ticks(8));

    svg
      .append("g")
      .attr("transform", `translate(${margin.left},0)`)
      .call(d3.axisLeft(y));

    SERIES.forEach((series) => {
      const line = d3
        .line()
        .x((d) => x(d.date))
        .y((d) => y(d[series.key]));

      svg
        .append("path")
        .datum(parsed)
        .attr("fill", "none")
        .attr("stroke", series.color)
        .attr("stroke-width", 2)
        .attr("d", line);
    });

    svg
      .append("text")
      .attr("x", margin.left)
      .attr("y", 14)
      .attr("fill", "#e5e7eb")
      .style("font-size", "14px")
      .text(title);
  }, [data, title]);

  return (
    <div>
      <div className="chart-wrap" ref={ref} />
      <div className="legend-row">
        {SERIES.map((s) => (
          <span key={s.key}>{s.label}</span>
        ))}
      </div>
    </div>
  );
}

export default MultiMetricLineChart;