import { useEffect, useRef } from "react";
import * as d3 from "d3";
import { getProvinceName } from "../../utils/provinceMap";

function SimpleBarChart({ data, title }) {
  const ref = useRef();

  useEffect(() => {
    const container = d3.select(ref.current);
    container.selectAll("*").remove();

    const displayData = data.map((d) => ({
      ...d,
      label: getProvinceName(d.label),
    }));

    const width = 860;
    const height = 340;
    const margin = { top: 24, right: 20, bottom: 80, left: 50 };

    const svg = container
      .append("svg")
      .attr("width", width)
      .attr("height", height);

    const x = d3
      .scaleBand()
      .domain(displayData.map((d) => d.label))
      .range([margin.left, width - margin.right])
      .padding(0.2);

    const y = d3
      .scaleLinear()
      .domain([0, d3.max(displayData, (d) => d.value) || 0])
      .nice()
      .range([height - margin.bottom, margin.top]);

    svg
      .append("g")
      .attr("transform", `translate(0,${height - margin.bottom})`)
      .call(d3.axisBottom(x))
      .selectAll("text")
      .attr("transform", "rotate(-20)")
      .style("text-anchor", "end");

    svg
      .append("g")
      .attr("transform", `translate(${margin.left},0)`)
      .call(d3.axisLeft(y));

    svg
      .selectAll("rect")
      .data(displayData)
      .enter()
      .append("rect")
      .attr("x", (d) => x(d.label))
      .attr("y", (d) => y(d.value))
      .attr("width", x.bandwidth())
      .attr("height", (d) => y(0) - y(d.value))
      .attr("fill", "#8b5cf6");

    svg
      .append("text")
      .attr("x", margin.left)
      .attr("y", 14)
      .attr("fill", "#e5e7eb")
      .style("font-size", "14px")
      .text(title);
  }, [data, title]);

  return <div className="chart-wrap" ref={ref} />;
}

export default SimpleBarChart;