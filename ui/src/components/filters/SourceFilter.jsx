function SourceFilter({ value, onChange, options }) {
  return (
    <select value={value} onChange={(e) => onChange(e.target.value)}>
      {options.map((item) => (
        <option key={item.value} value={item.value}>
          {item.label}
        </option>
      ))}
    </select>
  );
}

export default SourceFilter;