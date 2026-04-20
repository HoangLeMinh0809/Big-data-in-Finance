import { useEffect, useRef } from "react";
import * as d3 from "d3";

function RealtimeLineChart({ data, metric, color = "#38bdf8", title }) {
  const ref = useRef();

  useEffect(() => {
    const container = d3.select(ref.current);
    container.selectAll("*").remove();

    const width = 860;
    const height = 320;
    const margin = { top: 20, right: 30, bottom: 40, left: 50 };

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
      .call(d3.axisBottom(x).ticks(6));

    svg
      .append("g")
      .attr("transform", `translate(${margin.left},0)`)
      .call(d3.axisLeft(y));

    svg
      .append("path")
      .datum(parsed)
      .attr("fill", "none")
      .attr("stroke", color)
      .attr("stroke-width", 2.5)
      .attr("d", line);

    svg
      .selectAll("circle")
      .data(parsed)
      .enter()
      .append("circle")
      .attr("cx", (d) => x(d.date))
      .attr("cy", (d) => y(d.value))
      .attr("r", 4)
      .attr("fill", color);

    svg
      .append("text")
      .attr("x", margin.left)
      .attr("y", 14)
      .attr("fill", "#e5e7eb")
      .style("font-size", "14px")
      .text(title);
  }, [data, metric, color, title]);

  return <div className="chart-wrap" ref={ref} />;
}

export default RealtimeLineChart;