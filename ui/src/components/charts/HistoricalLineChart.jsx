import { useEffect, useRef } from "react";
import * as d3 from "d3";

function HistoricalLineChart({ data, metric, title }) {
  const ref = useRef();

  useEffect(() => {
    const container = d3.select(ref.current);
    container.selectAll("*").remove();

    const width = 960;
    const height = 360;
    const margin = { top: 24, right: 30, bottom: 40, left: 60 };

    const svg = container
      .append("svg")
      .attr("width", width)
      .attr("height", height);

    const parsed = data.map((d) => ({
      ...d,
      date: new Date(d.timestamp),
      value: Number(d[metric]),
    }));

    const x = d3
      .scaleTime()
      .domain(d3.extent(parsed, (d) => d.date))
      .range([margin.left, width - margin.right]);

    const y = d3
      .scaleLinear()
      .domain([d3.min(parsed, (d) => d.value) - 2, d3.max(parsed, (d) => d.value) + 2])
      .nice()
      .range([height - margin.bottom, margin.top]);

    const line = d3
      .line()
      .x((d) => x(d.date))
      .y((d) => y(d.value));

    svg
      .append("g")
      .attr("transform", `translate(0,${height - margin.bottom})`)
      .call(d3.axisBottom(x).ticks(8));

    svg
      .append("g")
      .attr("transform", `translate(${margin.left},0)`)
      .call(d3.axisLeft(y));

    svg
      .append("path")
      .datum(parsed)
      .attr("fill", "none")
      .attr("stroke", "#22c55e")
      .attr("stroke-width", 2.5)
      .attr("d", line);

    svg
      .append("text")
      .attr("x", margin.left)
      .attr("y", 14)
      .attr("fill", "#e5e7eb")
      .style("font-size", "14px")
      .text(title);
  }, [data, metric, title]);

  return <div className="chart-wrap" ref={ref} />;
}

export default HistoricalLineChart;